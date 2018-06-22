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
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.generatePartitions;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestJoinEnumerator
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder().build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testGeneratePartitions()
    {
        Set<Set<Integer>> partitions = generatePartitions(4);
        assertEquals(partitions,
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2),
                        ImmutableSet.of(0, 3),
                        ImmutableSet.of(0, 1, 2),
                        ImmutableSet.of(0, 1, 3),
                        ImmutableSet.of(0, 2, 3)));

        partitions = generatePartitions(3);
        assertEquals(partitions,
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2)));
    }

    @Test
    public void testDoesNotCreateJoinWhenPartitionedOnCrossJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        MultiJoinNode multiJoinNode = new MultiJoinNode(
                ImmutableSet.of(planBuilder.values(a1), planBuilder.values(b1)),
                TRUE_LITERAL,
                ImmutableList.of(a1, b1));
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        CachingStatsProvider statsProvider = new CachingStatsProvider(
                queryRunner.getStatsCalculator(),
                Optional.empty(),
                noLookup(),
                queryRunner.getDefaultSession(),
                symbolAllocator::getTypes);
        CachingCostProvider costProvider = new CachingCostProvider(
                queryRunner.getCostCalculator(),
                statsProvider,
                Optional.empty(),
                noLookup(),
                queryRunner.getDefaultSession(),
                symbolAllocator::getTypes);
        ReorderJoins.JoinEnumerator joinEnumerator = new ReorderJoins.JoinEnumerator(
                queryRunner.getDefaultSession(),
                costProvider,
                new CostComparator(1, 1, 1),
                idAllocator,
                multiJoinNode.getFilter(),
                noLookup(),
                new Rule.Context()
                {
                    @Override
                    public Lookup getLookup()
                    {
                        return null;
                    }

                    @Override
                    public PlanNodeIdAllocator getIdAllocator()
                    {
                        return null;
                    }

                    @Override
                    public SymbolAllocator getSymbolAllocator()
                    {
                        return null;
                    }

                    @Override
                    public Session getSession()
                    {
                        return null;
                    }

                    @Override
                    public StatsProvider getStatsProvider()
                    {
                        return null;
                    }

                    @Override
                    public CostProvider getCostProvider()
                    {
                        return null;
                    }

                    @Override
                    public void checkTimeoutNotExhausted() {}
                });
        JoinEnumerationResult actual = joinEnumerator.createJoinAccordingToPartitioning(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols(), ImmutableSet.of(0));
        assertFalse(actual.getPlanNode().isPresent());
    }
}
