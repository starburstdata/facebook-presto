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
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCH queries.
 * This class is using TPCH connector configured in way to mock Hive connector with unpartitioned TPCH tables.
 */
public class TestTpcdsJoinReordering
        extends BaseJoinReorderingTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpcdsJoinReordering()
    {
        super(
                "sf3000.0",
                ImmutableMap.of(
                        JOIN_REORDERING_STRATEGY, COST_BASED.name(),
                        JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name()));
    }

    @Override
    protected LocalQueryRunner createQueryRunner(Session session)
    {
        return LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(session, 8);
    }

    @Override
    protected void createTpchCatalog(LocalQueryRunner queryRunner)
    {
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpcdsConnectorFactory(1),
                ImmutableMap.of());
    }

    @DataProvider
    public Object[][] getTpcdsQueryIds()
    {
        return IntStream.range(1, 100)
                .boxed()
                .flatMap(i -> {
                    if (i < 10) {
                        return Stream.of("0" + i);
                    }
                    if (i == 14 || i == 23 || i == 24 || i == 39) {
                        return Stream.of(i + "_1", i + "_2");
                    }
                    return Stream.of(i.toString());
                })
                .map(i -> new Object[]{i})
                .collect(toList())
                .toArray(new Object[][]{});
    }

    @Test(dataProvider = "getTpcdsQueryIds")
    public void test(String queryId)
            throws IOException
    {
        assertEquals(joinOrderString(tpcdsQuery(queryId)), read(getExpectedJoinOrderingFile(queryId)));
    }

    private String read(File file)
            throws IOException
    {
        return Joiner.on("\n").join(Files.readLines(file, UTF_8)) + "\n";
    }

    private static String tpcdsQuery(String queryId)
    {
        try {
            String queryPath = format("/tpcds/queries/q%s.sql", queryId);
            URL resourceUrl = Resources.getResource(TpcdsConnectorFactory.class, queryPath);
            return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(dataProvider = "getTpcdsQueryIds", enabled = false)
    public void update(String queryId)
            throws IOException
    {
        Files.write(joinOrderString(tpcdsQuery(queryId)), getExpectedJoinOrderingFile(queryId), UTF_8);
    }

    private static File getExpectedJoinOrderingFile(String queryId)
    {
        return new File(String.format("src/test/resources/join_ordering/%s.join_ordering", queryId));
    }
}
