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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TopNMatcher
        implements Matcher
{
    private final long count;
    private final ImmutableMap<String, SortOrder> orderingScheme;

    public TopNMatcher(long count, ImmutableMap<String, SortOrder> orderingScheme)
    {
        this.count = count;
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TopNNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeStatsEstimate planNodeStatsEstimate, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        TopNNode topNNode = (TopNNode) node;

        if (topNNode.getCount() != count) {
            return NO_MATCH;
        }

        for (String alias : orderingScheme.keySet()) {
            Symbol symbol = Symbol.from(symbolAliases.get(alias));
            OrderingScheme nodeOrderingScheme = topNNode.getOrderingScheme();
            if (!orderingScheme.get(alias).equals(nodeOrderingScheme.getOrderings().get(symbol))) {
                return NO_MATCH;
            }
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("count", count)
                .add("orderingScheme", orderingScheme)
                .toString();
    }
}
