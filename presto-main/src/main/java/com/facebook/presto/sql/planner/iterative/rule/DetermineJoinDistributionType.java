/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());

    private final CostComparator costComparator;

    public DetermineJoinDistributionType(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());

        if (joinDistributionType == AUTOMATIC) {
            PlanEnumeration.Result bestJoin = getCostBasedJoin(joinNode, context);
            if (bestJoin.isCostKnown()) {
                return Result.ofPlanNode(bestJoin.getPlanNode().get());
            }
        }

        return Result.ofPlanNode(getSyntacticOrderJoin(joinNode, context, joinDistributionType));
    }

    private PlanEnumeration.Result getCostBasedJoin(JoinNode joinNode, Context context)
    {
        PlanEnumeration planEnumeration = new PlanEnumeration.Factory(costComparator, context.getCostProvider(), context.getSession()).create();

        JoinNode.Type type = joinNode.getType();
        if (shouldRepartition(joinNode, AUTOMATIC, context)) {
            JoinNode possibleJoinNode = joinNode.withDistributionType(PARTITIONED);
            planEnumeration.enumerate(possibleJoinNode);

            if (SystemSessionProperties.getJoinReorderingStrategy(context.getSession()) == COST_BASED) {
                planEnumeration.enumerate(possibleJoinNode.flipChildren());
            }
        }

        if (type != FULL) {
            // RIGHT OUTER JOIN only works with hash partitioned data.
            if (type != RIGHT) {
                planEnumeration.enumerate(joinNode.withDistributionType(REPLICATED));
            }

            if (SystemSessionProperties.getJoinReorderingStrategy(context.getSession()) == COST_BASED) {
                // Don't flip LEFT OUTER JOIN, as RIGHT OUTER JOIN only works with hash partitioned data.
                if (type != LEFT) {
                    planEnumeration.enumerate(joinNode.flipChildren().withDistributionType(REPLICATED));
                }
            }
        }

        return planEnumeration.getResult();
    }

    private PlanNode getSyntacticOrderJoin(JoinNode node, Context context, JoinDistributionType joinDistributionType)
    {
        if (shouldRepartition(node, joinDistributionType, context)) {
            return node.withDistributionType(PARTITIONED);
        }
        return node.withDistributionType(REPLICATED);
    }

    private static boolean shouldRepartition(JoinNode node, JoinDistributionType joinDistributionType, Context context)
    {
        JoinNode.Type type = node.getType();
        if (type == RIGHT || type == FULL) {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return true;
        }

        if (mustBroadcastJoin(node, context)) {
            return false;
        }

        return joinDistributionType.canRepartition();
    }

    private static boolean mustBroadcastJoin(JoinNode node, Context context)
    {
        JoinNode.Type type = node.getType();
        if (node.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // There is nothing to partition on
            return true;
        }
        return isAtMostScalar(node.getRight(), context.getLookup());
    }
}
