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
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCH queries.
 * This class is using TPCH connector configured in way to mock Hive connector with unpartitioned TPCH tables.
 */
public class TestReorderJoinsAndDetermineDistributionType
        extends BasePlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestReorderJoinsAndDetermineDistributionType()
    {
        super(
                "sf3000.0",
                false,
                ImmutableMap.of(
                        JOIN_REORDERING_STRATEGY, COST_BASED.name(),
                        JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name()));
    }

    @Override
    protected LocalQueryRunner createQueryRunner(Session session)
    {
        return LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(session, 8);
    }

    // no joins in q1

    @Test
    public void testTpchQ02()
    {
        assertJoinOrder(
                "SELECT s.acctbal, s.name, n.name, p.partkey, p.mfgr, s.address, s.phone, s.comment " +
                        "FROM part p, supplier s, partsupp ps, nation n, region r " +
                        "WHERE" +
                        "  p.partkey = ps.partkey AND s.suppkey = ps.suppkey AND p.size = 15 AND p.type like '%BRASS'" +
                        "  AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey AND r.name = 'EUROPE' AND ps.supplycost = (" +
                        "    SELECT min(ps.supplycost)" +
                        "    FROM partsupp ps, supplier s, nation n, region r" +
                        "    WHERE p.partkey = ps.partkey AND s.suppkey = ps.suppkey AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey AND r.name = 'EUROPE'" +
                        "  )" +
                        "ORDER BY s.acctbal desc, n.name, s.name, p.partkey",
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("partsupp"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("supplier"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region")))),
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("partsupp"),
                                                tableScan("part")),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("supplier"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region"))))),
                        new Values()));
    }

    @Test
    public void testTpchQ03()
    {
        assertJoinOrder(
                "SELECT " +
                        "l.orderkey, " +
                        "sum(l.extendedprice * (1 - l.discount)) AS revenue, " +
                        "o.orderdate, o.shippriority " +
                        "FROM customer AS c, orders AS o, lineitem AS l " +
                        "WHERE c.mktsegment = 'BUILDING' " +
                        "AND c.custkey = o.custkey " +
                        "AND l.orderkey = o.orderkey " +
                        "AND o.orderdate<DATE '1995-03-15' " +
                        "AND l.shipdate > DATE '1995-03-15' " +
                        "GROUP BY l.orderkey, o.orderdate, o.shippriority " +
                        "ORDER BY revenue DESC, o.orderdate LIMIT 10",
                new Join(
                        INNER,
                        REPLICATED,
                        tableScan("lineitem"),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("orders"),
                                tableScan("customer"))));
    }

    @Test
    public void testTpchQ04()
    {
        assertJoinOrder(
                "SELECT " +
                        "o.orderpriority, count(*) AS order_count " +
                        "FROM orders o " +
                        "WHERE" +
                        "  o.orderdate >= DATE '1993-07-01'" +
                        "  AND o.orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH" +
                        "  AND EXISTS (" +
                        "    SELECT * " +
                        "    FROM lineitem l" +
                        "    WHERE l.orderkey = o.orderkey AND l.commitdate < l.receiptdate" +
                        "  )" +
                        "GROUP BY o.orderpriority " +
                        "ORDER BY o.orderpriority",
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                tableScan("lineitem"),
                                tableScan("orders")),
                        new Values()));
    }

    @Test
    public void testTpchQ05()
    {
        assertJoinOrder(
                "SELECT " +
                        "  n.name, " +
                        "  sum(l.extendedprice * (1 - l.discount)) AS revenue " +
                        "FROM " +
                        "  customer AS c, " +
                        "  orders AS o, " +
                        "  lineitem AS l, " +
                        "  supplier AS s, " +
                        "  nation AS n, " +
                        "  region AS r " +
                        "WHERE " +
                        "  c.custkey = o.custkey " +
                        "  AND l.orderkey = o.orderkey " +
                        "  AND l.suppkey = s.suppkey " +
                        "  AND c.nationkey = s.nationkey " +
                        "  AND s.nationkey = n.nationkey " +
                        "  AND n.regionkey = r.regionkey " +
                        "  AND r.name = 'ASIA' " +
                        "  AND o.orderdate >= DATE '1994-01-01' " +
                        "  AND o.orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR " +
                        "GROUP BY " +
                        "  n.name " +
                        "ORDER BY " +
                        "  revenue DESC ",
                new Join(
                        INNER,
                        REPLICATED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        tableScan("orders"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("customer"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region"))))),
                        tableScan("supplier")));
    }

    // no joins in q6

    @Test
    public void testTpchQ07()
    {
        assertJoinOrder(
                "SELECT" +
                        "  supp_nation, cust_nation, l_year, sum(volume) AS revenue " +
                        "FROM (" +
                        "       SELECT" +
                        "         n1.name                          AS supp_nation," +
                        "         n2.name                          AS cust_nation," +
                        "         extract(YEAR FROM l.shipdate)      AS l_year," +
                        "         l.extendedprice * (1 - l.discount) AS volume" +
                        "       FROM" +
                        "         supplier AS s," +
                        "         lineitem AS l," +
                        "         orders AS o," +
                        "         customer AS c," +
                        "         nation AS n1," +
                        "         nation AS n2" +
                        "       WHERE" +
                        "         s.suppkey = l.suppkey" +
                        "         AND o.orderkey = l.orderkey" +
                        "         AND c.custkey = o.custkey" +
                        "         AND s.nationkey = n1.nationkey" +
                        "         AND c.nationkey = n2.nationkey" +
                        "         AND (" +
                        "           (n1.name = 'FRANCE' AND n2.name = 'GERMANY')" +
                        "           OR (n1.name = 'GERMANY' AND n2.name = 'FRANCE')" +
                        "         )" +
                        "         AND l.shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'" +
                        "     ) AS shipping " +
                        "GROUP BY supp_nation, cust_nation, l_year " +
                        "ORDER BY supp_nation, cust_nation, l_year",
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("orders"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("customer"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ08()
    {
        assertJoinOrder(
                "SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share " +
                        "FROM (" +
                        "       SELECT" +
                        "         extract(YEAR FROM o.orderdate)     AS o_year," +
                        "         l.extendedprice * (1 - l.discount) AS volume," +
                        "         n2.name                          AS nation" +
                        "       FROM" +
                        "         part AS p," +
                        "         supplier AS s," +
                        "         lineitem AS l," +
                        "         orders AS o," +
                        "         customer AS c," +
                        "         nation AS n1," +
                        "         nation AS n2," +
                        "         region AS r" +
                        "       WHERE" +
                        "         p.partkey = l.partkey" +
                        "         AND s.suppkey = l.suppkey" +
                        "         AND l.orderkey = o.orderkey" +
                        "         AND o.custkey = c.custkey" +
                        "         AND c.nationkey = n1.nationkey" +
                        "         AND n1.regionkey = r.regionkey" +
                        "         AND r.name = 'AMERICA'" +
                        "         AND s.nationkey = n2.nationkey" +
                        "         AND o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'" +
                        "         AND p.type = 'ECONOMY ANODIZED STEEL'" +
                        "     ) AS all_nations " +
                        "GROUP BY o_year " +
                        "ORDER BY o_year",
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,

                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("orders"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("customer"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region")))),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("lineitem"),
                                        tableScan("part"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("supplier"),
                                tableScan("nation"))));
    }

    @Test
    public void testTpchQ09()
    {
        assertJoinOrder(
                "SELECT nation, o_year, sum(amount) AS sum_profit " +
                        "FROM (" +
                        "       SELECT" +
                        "         n.name                                                          AS nation," +
                        "         extract(YEAR FROM o.orderdate)                                  AS o_year," +
                        "         l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity AS amount" +
                        "       FROM" +
                        "         part AS p," +
                        "         supplier AS s," +
                        "         lineitem AS l," +
                        "         partsupp AS ps," +
                        "         orders AS o," +
                        "         nation AS n" +
                        "       WHERE" +
                        "         s.suppkey = l.suppkey" +
                        "         AND ps.suppkey = l.suppkey" +
                        "         AND ps.partkey = l.partkey" +
                        "         AND p.partkey = l.partkey" +
                        "         AND o.orderkey = l.orderkey" +
                        "         AND s.nationkey = n.nationkey" +
                        "         AND p.name LIKE '%green%'" +
                        "     ) AS profit " +
                        "GROUP BY nation, o_year " +
                        "ORDER BY nation, o_year DESC",
                new Join(
                        INNER,
                        REPLICATED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("lineitem"),
                                tableScan("orders")),
                        new Join(
                                INNER,
                                REPLICATED,
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("partsupp"),
                                        tableScan("part")),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ10()
    {
        assertJoinOrder(
                "SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue, c.acctbal, n.name, c.address, c.phone, c.comment " +
                        "FROM" +
                        "  lineitem AS l," +
                        "  orders AS o," +
                        "  customer AS c," +
                        "  nation AS n " +
                        "WHERE" +
                        "  c.custkey = o.custkey" +
                        "  AND l.orderkey = o.orderkey" +
                        "  AND o.orderdate >= DATE '1993-10-01'" +
                        "  AND o.orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH" +
                        "  AND l.returnflag = 'R'" +
                        "  AND c.nationkey = n.nationkey " +
                        "GROUP BY c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment " +
                        "ORDER BY revenue DESC " +
                        "LIMIT 20",
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("customer"),
                                tableScan("nation")),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                tableScan("orders"))));
    }

    @Test
    public void testTpchQ11()
    {
        assertJoinOrder(
                "SELECT ps.partkey, sum(ps.supplycost*ps.availqty) AS value " +
                        "FROM" +
                        "  partsupp ps," +
                        "  supplier s," +
                        "  nation n " +
                        "WHERE" +
                        "  ps.suppkey = s.suppkey" +
                        "  AND s.nationkey = n.nationkey" +
                        "  AND n.name = 'GERMANY' " +
                        "GROUP BY ps.partkey " +
                        "HAVING" +
                        "  sum(ps.supplycost*ps.availqty) > (" +
                        "    SELECT" +
                        "      sum(ps.supplycost*ps.availqty) * 0.0001000000" +
                        "    FROM" +
                        "      partsupp ps," +
                        "      supplier s," +
                        "      nation n" +
                        "    WHERE" +
                        "      ps.suppkey = s.suppkey" +
                        "      AND s.nationkey = n.nationkey" +
                        "      AND n.name = 'GERMANY'" +
                        "  )" +
                        "ORDER BY value DESC",
                crossJoin(
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("partsupp"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("partsupp"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ12()
    {
        assertJoinOrder(
                "SELECT" +
                        "  l.shipmode," +
                        "  sum(CASE" +
                        "      WHEN o.orderpriority = '1-URGENT'" +
                        "           OR o.orderpriority = '2-HIGH'" +
                        "        THEN 1" +
                        "      ELSE 0" +
                        "      END) AS high_line_count," +
                        "  sum(CASE" +
                        "      WHEN o.orderpriority <> '1-URGENT'" +
                        "           AND o.orderpriority <> '2-HIGH'" +
                        "        THEN 1" +
                        "      ELSE 0" +
                        "      END) AS low_line_count " +
                        "FROM" +
                        "  orders AS o," +
                        "  lineitem AS l " +
                        "WHERE" +
                        "  o.orderkey = l.orderkey" +
                        "  AND l.shipmode IN ('MAIL', 'SHIP')" +
                        "  AND l.commitdate < l.receiptdate" +
                        "  AND l.shipdate < l.commitdate" +
                        "  AND l.receiptdate >= DATE '1994-01-01'" +
                        "  AND l.receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR " +
                        "GROUP BY l.shipmode " +
                        "ORDER BY l.shipmode",
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("orders"),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ13()
    {
        assertJoinOrder(
                "SELECT c_count, count(*) as custdist " +
                        "FROM (" +
                        "  SELECT" +
                        "    c.custkey," +
                        "    count(o.orderkey)" +
                        "  FROM" +
                        "    customer c" +
                        "    LEFT OUTER JOIN" +
                        "    orders o" +
                        "  ON" +
                        "    c.custkey = o.custkey" +
                        "    AND o.comment NOT LIKE '%special%requests%'" +
                        "  GROUP BY c.custkey" +
                        ") AS c_orders (c_custkey, c_count)" +
                        "GROUP BY c_count " +
                        "ORDER BY custdist DESC, c_count DESC",
                new Join(
                        RIGHT,
                        PARTITIONED,
                        tableScan("orders"),
                        tableScan("customer")));
    }

    @Test
    public void testTpchQ14()
    {
        assertJoinOrder(
                "SELECT 100.00 * sum(CASE" +
                        "  WHEN p.type LIKE 'PROMO%'" +
                        "    THEN l.extendedprice * (1 - l.discount)" +
                        "  ELSE 0" +
                        "  END) / sum(l.extendedprice * (1 - l.discount)) AS promo_revenue " +
                        "FROM" +
                        "  lineitem AS l," +
                        "  part AS p " +
                        "WHERE" +
                        "  l.partkey = p.partkey" +
                        "  AND l.shipdate >= DATE '1995-09-01'" +
                        "  AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH",
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("part"),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ15()
    {
        assertJoinOrder(
                "WITH revenue0 AS (" +
                        "  SELECT   l.suppkey as supplier_no,   sum(l.extendedprice*(1-l.discount)) as total_revenue" +
                        "  FROM" +
                        "    lineitem l" +
                        "  WHERE" +
                        "    l.shipdate >= DATE '1996-01-01'" +
                        "    AND l.shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH" +
                        "  GROUP BY   l.suppkey" +
                        ")" +
                        "SELECT s.suppkey, s.name, s.address, s.phone, total_revenue " +
                        "FROM" +
                        "  supplier s," +
                        "  revenue0 " +
                        "WHERE" +
                        "  s.suppkey = supplier_no" +
                        "  AND total_revenue = (SELECT max(total_revenue) FROM revenue0)" +
                        "ORDER BY s.suppkey",
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("supplier"),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                tableScan("lineitem"))));
    }

    @Test
    public void testTpchQ16()
    {
        assertJoinOrder(
                "SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt " +
                        "FROM" +
                        "  partsupp AS ps," +
                        "  part AS p " +
                        "WHERE" +
                        "  p.partkey = ps.partkey" +
                        "  AND p.brand <> 'Brand#45'" +
                        "  AND p.type NOT LIKE 'MEDIUM POLISHED%'" +
                        "  AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)" +
                        "  AND ps.suppkey NOT IN (" +
                        "    SELECT s.suppkey" +
                        "    FROM     supplier AS s" +
                        "    WHERE     s.comment LIKE '%Customer%Complaints%'" +
                        "  )" +
                        "GROUP BY p.brand, p.type, p.size " +
                        "ORDER BY supplier_cnt DESC, p.brand, p.type, p.size",
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("partsupp"),
                                tableScan("part")),
                        tableScan("supplier")));
    }

    @Test
    public void testTpchQ17()
    {
        assertJoinOrder(
                "SELECT sum(l.extendedprice)/7.0 as avg_yearly " +
                        "FROM" +
                        "  lineitem l," +
                        "  part p " +
                        "WHERE" +
                        "  p.partkey = l.partkey" +
                        "  AND p.brand = 'Brand#23'" +
                        "  AND p.container = 'MED BOX'" +
                        "  AND l.quantity < (" +
                        "    SELECT     0.2*avg(l.quantity)" +
                        "    FROM     lineitem l" +
                        "    WHERE   l.partkey = p.partkey" +
                        "  )",
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("lineitem"),
                                        tableScan("part"))),
                        new Values()));
    }

    @Test
    public void testTpchQ18()
    {
        assertJoinOrder(
                "SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) " +
                        "FROM" +
                        "  customer AS c," +
                        "  orders AS o," +
                        "  lineitem AS l " +
                        "WHERE" +
                        "  o.orderkey IN (" +
                        "    SELECT l.orderkey" +
                        "    FROM     lineitem AS l" +
                        "    GROUP BY     l.orderkey" +
                        "    HAVING     sum(l.quantity) > 300" +
                        "  )" +
                        "  AND c.custkey = o.custkey" +
                        "  AND o.orderkey = l.orderkey " +
                        "GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice " +
                        "ORDER BY o.totalprice DESC, o.orderdate " +
                        "LIMIT 100",
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("orders"),
                                        tableScan("customer"))),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ19()
    {
        assertJoinOrder(
                "SELECT sum(l.extendedprice* (1 - l.discount)) as revenue " +
                        "FROM" +
                        "  lineitem l," +
                        "  part p " +
                        "WHERE" +
                        "  p.partkey = l.partkey" +
                        "  AND" +
                        "  ((" +
                        "    p.brand = 'Brand#12'" +
                        "    AND p.container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')" +
                        "    AND l.quantity >= 1" +
                        "    AND l.quantity <= 1 + 10" +
                        "    AND p.size BETWEEN 1 AND 5" +
                        "    AND l.shipmode IN ('AIR', 'AIR REG')" +
                        "    AND l.shipinstruct = 'DELIVER IN PERSON'" +
                        "  )" +
                        "  OR (" +
                        "    p.brand ='Brand#23'" +
                        "    AND p.container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')" +
                        "    AND l.quantity >=10" +
                        "    AND l.quantity <=10 + 10" +
                        "    AND p.size BETWEEN 1 AND 10" +
                        "    AND l.shipmode IN ('AIR', 'AIR REG')" +
                        "    AND l.shipinstruct = 'DELIVER IN PERSON'" +
                        "  )" +
                        "  OR (" +
                        "    p.brand = 'Brand#34'" +
                        "    AND p.container IN ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')" +
                        "    AND l.quantity >=20" +
                        "    AND l.quantity <= 20 + 10" +
                        "    AND p.size BETWEEN 1 AND 15" +
                        "    AND l.shipmode IN ('AIR', 'AIR REG')" +
                        "    AND l.shipinstruct = 'DELIVER IN PERSON'" +
                        "  ))",
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("lineitem"),
                        tableScan("part")));
    }

    @Test
    public void testTpchQ20()
    {
        assertJoinOrder(
                "SELECT s.name, s.address " +
                        "FROM supplier s, nation n " +
                        "WHERE" +
                        "  s.suppkey IN (" +
                        "    SELECT     ps.suppkey" +
                        "    FROM     partsupp ps" +
                        "    WHERE" +
                        "      ps.partkey IN (" +
                        "        SELECT         p.partkey" +
                        "        FROM         part p" +
                        "        WHERE         p.name like 'forest%'" +
                        "      )" +
                        "      AND ps.availqty > (" +
                        "        SELECT         0.5*sum(l.quantity)" +
                        "        FROM         lineitem l" +
                        "        WHERE" +
                        "          l.partkey = ps.partkey" +
                        "          AND l.suppkey = ps.suppkey" +
                        "          AND l.shipdate >= date('1994-01-01')" +
                        "          AND l.shipdate < date('1994-01-01') + interval '1' YEAR" +
                        "      )" +
                        "  )" +
                        "  AND s.nationkey = n.nationkey" +
                        "  AND n.name = 'CANADA' " +
                        "ORDER BY s.name",
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("supplier"),
                                tableScan("nation")),
                        crossJoin(
                                new Join(
                                        RIGHT,
                                        PARTITIONED,
                                        tableScan("lineitem"),
                                        new SemiJoin(
                                                PARTITIONED,
                                                tableScan("partsupp"),
                                                tableScan("part"))),
                                new Values())));
    }

    @Test
    public void testTpchQ21()
    {
        assertJoinOrder(
                "SELECT s.name, count(*) as numwait " +
                        "FROM" +
                        "  supplier s," +
                        "  lineitem l1," +
                        "  orders o," +
                        "  nation n " +
                        "WHERE" +
                        "  s.suppkey = l1.suppkey" +
                        "  AND o.orderkey = l1.orderkey" +
                        "  AND o.orderstatus = 'F'" +
                        "  AND l1.receiptdate> l1.commitdate" +
                        "  AND EXISTS (" +
                        "    SELECT *" +
                        "    FROM lineitem l2" +
                        "    WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey" +
                        "  )" +
                        "  AND NOT EXISTS (" +
                        "    SELECT *" +
                        "    FROM lineitem l3" +
                        "    WHERE l3.orderkey = l1.orderkey AND l3.suppkey <> l1.suppkey AND l3.receiptdate > l3.commitdate" +
                        "  )" +
                        "  AND s.nationkey = n.nationkey" +
                        "  AND n.name = 'SAUDI ARABIA' " +
                        "GROUP BY s.name " +
                        "ORDER BY numwait DESC, s.name " +
                        "LIMIT 100",
                new Join(
                        LEFT,
                        PARTITIONED,
                        new Join(
                                LEFT,
                                PARTITIONED,
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("lineitem"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("supplier"),
                                                        tableScan("nation"))),
                                        tableScan("orders")),
                                tableScan("lineitem")),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ22()
    {
        assertJoinOrder(
                "SELECT cntrycode, count(*) AS numcust, sum(acctbal) AS totacctbal " +
                        "FROM" +
                        "  (" +
                        "    SELECT substr(c.phone,1,2) AS cntrycode, c.acctbal" +
                        "    FROM customer c" +
                        "    WHERE" +
                        "      substr(c.phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')" +
                        "      AND c.acctbal > (" +
                        "        SELECT avg(c.acctbal)" +
                        "        FROM customer c" +
                        "        WHERE c.acctbal > 0.00 AND substr(c.phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')" +
                        "      )" +
                        "      AND NOT EXISTS (" +
                        "        SELECT *" +
                        "        FROM orders o" +
                        "        WHERE o.custkey = c.custkey" +
                        "      )" +
                        "  ) AS custsale " +
                        "GROUP BY cntrycode " +
                        "ORDER BY cntrycode",
                crossJoin(
                        new Join(
                                LEFT,
                                PARTITIONED,
                                crossJoin(
                                        tableScan("customer"),
                                        tableScan("customer")),
                                tableScan("orders")),
                        new Values()));
    }

    private TableScan tableScan(String tableName)
    {
        return new TableScan(format("tpch:%s:%s", tableName, getQueryRunner().getDefaultSession().getSchema().get()));
    }

    private void assertJoinOrder(String sql, Node expected)
    {
        assertEquals(joinOrderString(sql), expected.print());
    }

    private String joinOrderString(String sql)
    {
        Plan plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder stringBuilder = new StringBuilder();

        public String result()
        {
            return stringBuilder.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinNode.DistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new IllegalStateException("Expected distribution type to be present"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS join to be INNER REPLICATED");
                stringBuilder.append(indentString(indent))
                        .append("cross join:\n");
            }
            else {
                stringBuilder.append(indentString(indent))
                        .append("join (")
                        .append(node.getType())
                        .append(", ")
                        .append(distributionType)
                        .append("):\n");
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(node.getTable().getConnectorHandle().toString())
                    .append("\n");
            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("semijoin (")
                    .append(node.getDistributionType().map(SemiJoinNode.DistributionType::toString).orElse("unknown"))
                    .append("):\n");

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("values\n");

            return null;
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private interface Node
    {
        void print(StringBuilder stringBuilder, int indent);

        default String print()
        {
            StringBuilder stringBuilder = new StringBuilder();
            print(stringBuilder, 0);
            return stringBuilder.toString();
        }
    }

    private Join crossJoin(Node left, Node right)
    {
        return new Join(INNER, REPLICATED, true, left, right);
    }

    private static class Join
            implements Node
    {
        private final JoinNode.Type type;
        private final JoinNode.DistributionType distributionType;
        private final boolean isCrossJoin;
        private final Node left;
        private final Node right;

        private Join(JoinNode.Type type, JoinNode.DistributionType distributionType, Node left, Node right)
        {
            this(type, distributionType, false, left, right);

        }

        private Join(JoinNode.Type type, JoinNode.DistributionType distributionType, boolean isCrossJoin, Node left, Node right)
        {
            if (isCrossJoin) {
                checkArgument(distributionType == REPLICATED && type == INNER, "Cross join can only accept INNER REPLICATED join");
            }
            this.type = requireNonNull(type, "type is null");
            this.distributionType = requireNonNull(distributionType, "distributionType is null");
            this.isCrossJoin = isCrossJoin;
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            if (isCrossJoin) {
                stringBuilder.append(indentString(indent))
                        .append("cross join:\n");
            }
            else {
                stringBuilder.append(indentString(indent))
                        .append("join (")
                        .append(type)
                        .append(", ")
                        .append(distributionType)
                        .append("):\n");
            }

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class SemiJoin
            implements Node
    {
        private final JoinNode.DistributionType distributionType;
        private final Node left;
        private final Node right;

        private SemiJoin(JoinNode.DistributionType distributionType, final Node left, final Node right)
        {
            this.distributionType = requireNonNull(distributionType);
            this.left = requireNonNull(left);
            this.right = requireNonNull(right);
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("semijoin (")
                    .append(distributionType.toString())
                    .append("):\n");

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class TableScan
            implements Node
    {
        private final String tableName;

        private TableScan(String tableName)
        {
            this.tableName = tableName;
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(tableName)
                    .append("\n");
        }
    }

    private static class Values
            implements Node
    {
        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("values\n");
        }
    }
}
