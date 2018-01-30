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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_SORT;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestDistributedSort
        extends BasePlanTest
{
    @Test
    public void testSimpleDistributedSort()
    {
        ImmutableMap<String, SortOrder> orderingScheme = ImmutableMap.of("ORDERKEY", ASC_NULLS_LAST);
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", distributedSortNoRedistribution(), false,
                anyTree(
                        exchange(REMOTE, GATHER, Optional.of(orderingScheme),
                                exchange(LOCAL, GATHER, Optional.of(orderingScheme),
                                        sort(orderingScheme,
                                                exchange(LOCAL, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testSimpleDistributedSortWithRedistribution()
    {
        ImmutableMap<String, SortOrder> orderingScheme = ImmutableMap.of("ORDERKEY", DESC_NULLS_LAST);
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey DESC", distributedSortWithRedistribution(), false,
                anyTree(
                        exchange(REMOTE, GATHER, Optional.of(orderingScheme),
                                exchange(LOCAL, GATHER, Optional.of(orderingScheme),
                                        sort(orderingScheme,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    private Session distributedSortNoRedistribution()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(true))
                .setSystemProperty(REDISTRIBUTE_SORT, Boolean.toString(false))
                .build();
    }

    private Session distributedSortWithRedistribution()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(true))
                .build();
    }
}
