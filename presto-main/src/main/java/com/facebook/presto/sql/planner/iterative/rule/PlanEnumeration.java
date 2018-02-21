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

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.Ordering;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeCostEstimate.INFINITE_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PlanEnumeration
{
    public static final Result UNKNOWN_COST_RESULT = new Result(UNKNOWN_COST, Optional.empty());
    public static final Result INFINITE_COST_RESULT = new Result(INFINITE_COST, Optional.empty());

    private final CostProvider costProvider;
    private final Ordering<Result> ordering;
    private Result result = INFINITE_COST_RESULT;

    private PlanEnumeration(CostProvider costProvider, Ordering<Result> ordering)
    {
        this.costProvider = requireNonNull(costProvider, "costProvider is null");
        this.ordering = requireNonNull(ordering, "ordering is null");
    }

    public PlanEnumeration enumerate(PlanNode planNode)
    {
        requireNonNull(planNode, "planNode is null");
        if (result.cost.hasUnknownComponents()) {
            return this;
        }

        enumerate(new Result(costProvider.getCumulativeCost(planNode), Optional.of(planNode)));
        return this;
    }

    public PlanEnumeration enumerate(Result candidate)
    {
        requireNonNull(candidate, "candidate is null");
        if (candidate.getCost().hasUnknownComponents()) {
            result = UNKNOWN_COST_RESULT;
        }
        else {
            result = ordering.min(result, candidate);
        }
        return this;
    }

    public Result getResult()
    {
        return result;
    }

    public static class Result
    {
        private final Optional<PlanNode> planNode;
        private final PlanNodeCostEstimate cost;

        private Result(PlanNodeCostEstimate cost, Optional<PlanNode> planNode)
        {
            this.cost = requireNonNull(cost);
            this.planNode = requireNonNull(planNode);
            checkArgument(cost.hasUnknownComponents() || cost.equals(INFINITE_COST) || planNode.isPresent(), "planNode must be present if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanNodeCostEstimate getCost()
        {
            return cost;
        }

        public boolean isCostKnown()
        {
            return !cost.equals(INFINITE_COST) && !cost.hasUnknownComponents();
        }
    }

    public static class Factory
    {
        private final CostProvider costProvider;
        private final Ordering<Result> ordering;

        public Factory(CostComparator costComparator, CostProvider costProvider, Session session)
        {
            this.costProvider = requireNonNull(costProvider, "costProvider is null");
            this.ordering = getResultOrdering(costComparator, session);
        }

        private static Ordering<Result> getResultOrdering(CostComparator costComparator, Session session)
        {
            requireNonNull(costComparator, "costComparator is null");
            requireNonNull(session, "session is null");
            return new Ordering<Result>()
            {
                @Override
                public int compare(Result result1, Result result2)
                {
                    return costComparator.compare(session, result1.cost, result2.cost);
                }
            };
        }

        public PlanEnumeration create()
        {
            return new PlanEnumeration(costProvider, ordering);
        }

        public PlanEnumeration.Result enumerate(PlanNode... planNodes)
        {
            PlanEnumeration planEnumeration = create();
            Arrays.stream(planNodes).forEach(planEnumeration::enumerate);
            return planEnumeration.getResult();
        }
    }
}
