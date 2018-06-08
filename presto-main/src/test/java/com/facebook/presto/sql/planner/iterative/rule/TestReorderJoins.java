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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.SymbolStatsEstimate;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.REPARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestReorderJoins
{
    private RuleTester tester;

    // TWO_ROWS are used to prevent node to be scalar
    private static final ImmutableList<List<Expression>> TWO_ROWS = ImmutableList.of(ImmutableList.of(), ImmutableList.of());

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(
                        JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name(),
                        JOIN_REORDERING_STRATEGY, COST_BASED.name()),
                Optional.of(4));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testKeepsOutputSymbols()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("A2", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A2", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5000)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 100),
                                new Symbol("A2"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0, "A2", 1)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, REPARTITIONED.name())
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionsWhenBothTablesEqual()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatedScalarJoinEvenWhereSessionRequiresRepartitioned()
    {
        PlanMatchPattern expectdPlan = join(
                INNER,
                ImmutableList.of(equiJoinClause("A1", "B1")),
                Optional.empty(),
                Optional.of(REPLICATED),
                values(ImmutableMap.of("A1", 0)),
                values(ImmutableMap.of("B1", 0)));

        PlanNodeStatsEstimate valuesA = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                .build();
        PlanNodeStatsEstimate valuesB = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                .build();

        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, REPARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)), // matches isAtMostScalar
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", valuesA)
                .withStats("valuesB", valuesB)
                .matches(expectdPlan);

        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, REPARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)), // matches isAtMostScalar
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", valuesA)
                .withStats("valuesB", valuesB)
                .matches(expectdPlan);
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoStats()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .withStatsCalculator(new UnknownStatsCalculator())
                .on(p ->
                        p.join(
                                INNER,
                                p.tableScan(ImmutableList.of(p.symbol("A1", BIGINT)), ImmutableMap.of(p.symbol("A1", BIGINT), new TestingColumnHandle("A1"))),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonDeterministicFilter()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.of(new ComparisonExpression(ComparisonExpressionType.LESS_THAN, p.symbol("A1", BIGINT).toSymbolReference(), new FunctionCall(QualifiedName.of("random"), ImmutableList.of())))))
                .doesNotFire();
    }

    @Test
    public void testPredicatesPushedDown()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.symbol("A1", BIGINT)), TWO_ROWS),
                                        p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)), TWO_ROWS),
                                        ImmutableList.of(),
                                        ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), ImmutableList.of(p.symbol("C1", BIGINT)), TWO_ROWS),
                                ImmutableList.of(
                                        new JoinNode.EquiJoinClause(p.symbol("B2", BIGINT), p.symbol("C1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                Optional.of(new ComparisonExpression(EQUAL, p.symbol("A1", BIGINT).toSymbolReference(), p.symbol("B1", BIGINT).toSymbolReference()))))
                .withStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .withStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 10),
                                new Symbol("B2"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .withStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("C1", "B2")),
                                values("C1"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("A1", "B1")),
                                        values("A1"),
                                        values("B1", "B2"))));
    }

    private static class UnknownStatsCalculator
            implements StatsCalculator
    {
        @Override
        public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
            node.getOutputSymbols()
                    .forEach(symbol -> statsBuilder.addSymbolStatistics(symbol, SymbolStatsEstimate.UNKNOWN_STATS));
            return statsBuilder.build();
        }
    }
}
