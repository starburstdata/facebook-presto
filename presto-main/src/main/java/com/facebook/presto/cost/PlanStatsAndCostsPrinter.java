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
package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Strings;

import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static java.util.Objects.requireNonNull;

public class PlanStatsAndCostsPrinter
{
    public static String displayPlanStatsAndCost(PlanNode root, Optional<StatsProvider> statsProvider, Optional<CostProvider> costProvider)
    {
        return displayPlanStatsAndCost(root, statsProvider, costProvider, alwaysTrue());

    }
    public static String displayPlanStatsAndCost(PlanNode root, Optional<StatsProvider> statsProvider, Optional<CostProvider> costProvider, Predicate<Symbol> symbolPredicate)
    {
        DisplayingStatsVisitor.Context context = new DisplayingStatsVisitor.Context();
        root.accept(new DisplayingStatsVisitor(statsProvider, costProvider, symbolPredicate), context);
        return context.getOutput();
    }

    private static final class DisplayingStatsVisitor
            extends SimplePlanVisitor<DisplayingStatsVisitor.Context>
    {
        private Optional<StatsProvider> statsProvider;
        private Optional<CostProvider> costProvider;
        private Predicate<Symbol> symbolPredicate;

        public DisplayingStatsVisitor(Optional<StatsProvider> statsProvider, Optional<CostProvider> costProvider, Predicate<Symbol> symbolPredicate)
        {
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.symbolPredicate = requireNonNull(symbolPredicate, "symbolPredicate is null");
            this.costProvider = requireNonNull(costProvider, "costProvider is null");
        }

        @Override
        protected Void visitPlan(PlanNode node, Context context)
        {
            context.output.append(indentString(context.indent))
                    .append(node.getClass().getSimpleName())
                    .append(":")
                    .append("\n");

            if (costProvider.isPresent()) {
                PlanNodeCostEstimate cumulativeCost = costProvider.get().getCumulativeCost(node);

                context.output.append(indentString(context.indent + 1))
                        .append("cummulative cost: cpu: ")
                        .append(cumulativeCost.getCpuCost())
                        .append(", mem: ")
                        .append(cumulativeCost.getMemoryCost())
                        .append(", net: ")
                        .append(cumulativeCost.getNetworkCost())
                        .append("\n");
            }

            if (statsProvider.isPresent()) {
                PlanNodeStatsEstimate stats = statsProvider.get().getStats(node);

                context.output.append(indentString(context.indent + 1))
                        .append("row count: ")
                        .append(stats.getOutputRowCount())
                        .append("\n");

                stats.getSymbolsWithKnownStatistics().stream()
                        .filter(symbolPredicate)
                        .forEach(symbol -> displaySymbolStatistics(context, stats, symbol));
            }

            return super.visitPlan(node, context.nested());
        }

        private void displaySymbolStatistics(Context context, PlanNodeStatsEstimate stats, Symbol symbol)
        {
            SymbolStatsEstimate symbolStatistics = stats.getSymbolStatistics(symbol);
            context.output.append(indentString(context.indent + 1))
                    .append(symbol.getName())
                    .append(":")
                    .append(" low: ")
                    .append(symbolStatistics.getLowValue())
                    .append(", high: ")
                    .append(symbolStatistics.getHighValue())
                    .append(", null: ")
                    .append(symbolStatistics.getNullsFraction())
                    .append(", DVC: ")
                    .append(symbolStatistics.getDistinctValuesCount())
                    .append(", avg size: ")
                    .append(symbolStatistics.getAverageRowSize())
                    .append("\n");
        }

        private static String indentString(int indent)
        {
            return Strings.repeat("  ", indent);
        }

        static final class Context
        {
            private final int indent;
            private final StringBuilder output;

            public Context()
            {
                this(0, new StringBuilder());
            }

            public Context(int indent, StringBuilder output)
            {
                this.indent = indent;
                this.output = output;
            }

            public Context nested()
            {
                return new Context(indent + 1, output);
            }

            public String getOutput()
            {
                return output.toString();
            }
        }
    }
}
