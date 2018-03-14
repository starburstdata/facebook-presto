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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchangeBelowPartialAggregation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ENABLE_ADAPTIVE_LOCAL_EXCHANGE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestAddLocalExchangeBelowPartialAggregation
        extends BaseRuleTest
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testDoesNotFireForFinalAggregation()
    {
        tester().assertThat(new AddLocalExchangeBelowPartialAggregation(tester().getMetadata(), SQL_PARSER))
                .setSystemProperty(ENABLE_ADAPTIVE_LOCAL_EXCHANGE, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(p.values(p.symbol("GROUP_COL"), p.symbol("AGGR_COL")))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(AGGR_COL)"), ImmutableList.of(DOUBLE))
                        .addGroupingSet(p.symbol("GROUP_COL"))
                        .step(FINAL)))
                .doesNotFire();
    }

    @Test
    public void testDoesAddLocalHashExchangeBelowPartialAggregation()
    {
        tester().assertThat(new AddLocalExchangeBelowPartialAggregation(tester().getMetadata(), SQL_PARSER))
                .setSystemProperty(ENABLE_ADAPTIVE_LOCAL_EXCHANGE, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(p.values(p.symbol("GROUP_COL"), p.symbol("AGGR_COL")))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(AGGR_COL)"), ImmutableList.of(DOUBLE))
                        .addGroupingSet(p.symbol("GROUP_COL"))
                        .step(PARTIAL)))
                .matches(
                        aggregation(
                                ImmutableList.of(ImmutableList.of("GROUP_COL")),
                                ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("AGGR_COL"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                PARTIAL,
                                exchange(LOCAL, REPARTITION,
                                        values("GROUP_COL", "AGGR_COL"))));
    }

    @Test
    public void testDoesNotFireWhenAlreadyPartitioned()
    {
        tester().assertThat(new AddLocalExchangeBelowPartialAggregation(tester().getMetadata(), SQL_PARSER))
                .setSystemProperty(ENABLE_ADAPTIVE_LOCAL_EXCHANGE, "true")
                .on(p -> {
                    Symbol groupSymbol1 = p.symbol("GROUP_COL1");
                    Symbol groupSymbol2 = p.symbol("GROUP_COL2");
                    Symbol aggregationSymol = p.symbol("AGGR_COL");
                    Symbol averageSymbol = p.symbol("AVG", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(p.exchange(e -> e
                                    .addSource(p.values(groupSymbol1, groupSymbol2, aggregationSymol))
                                    .addInputsSet(groupSymbol1, groupSymbol2, aggregationSymol)
                                    .type(REPARTITION)
                                    .scope(LOCAL)
                                    .fixedHashDistributionParitioningScheme(
                                            ImmutableList.of(groupSymbol1, groupSymbol2, aggregationSymol),
                                            ImmutableList.of(groupSymbol1))))
                            .addAggregation(averageSymbol, expression("AVG(AGGR_COL)"), ImmutableList.of(DOUBLE))
                            .addGroupingSet(groupSymbol1, groupSymbol2)
                            .step(PARTIAL));
                })
                .doesNotFire();
    }
}
