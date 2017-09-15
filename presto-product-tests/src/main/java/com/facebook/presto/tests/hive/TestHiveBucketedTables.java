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
package com.facebook.presto.tests.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.Requirements;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.MutableTableRequirement;
import io.prestodb.tempto.fulfillment.table.TableDefinitionsRepository;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestodb.tempto.query.QueryExecutionException;
import io.prestodb.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.TableDefinitionUtils.mutableTableInstanceOf;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.prestodb.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestHiveBucketedTables
        extends ProductTest
        implements RequirementsProvider
{
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_NATION = bucketTableDefinition("bucket_nation", false, false);
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_EMPTY_NATION = bucketTableDefinition("bucket_empty_nation", false, false);
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_SORTED_NATION = bucketTableDefinition("bucket_sort_nation", true, false);
    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition BUCKETED_PARTITIONED_NATION = bucketTableDefinition("bucket_partition_nation", false, true);

    private static HiveTableDefinition bucketTableDefinition(String tableName, boolean sorted, boolean partitioned)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                        "n_nationkey     BIGINT," +
                        "n_name          STRING," +
                        "n_regionkey     BIGINT," +
                        "n_comment       STRING) " +
                        (partitioned ? "PARTITIONED BY (part_key STRING) " : " ") +
                        "CLUSTERED BY (n_regionkey) " +
                        (sorted ? "SORTED BY (n_regionkey) " : " ") +
                        "INTO 4 BUCKETS " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'")
                .setNoData()
                .build();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return Requirements.compose(
                MutableTableRequirement.builder(BUCKETED_EMPTY_NATION).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_PARTITIONED_NATION).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_NATION).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_SORTED_NATION).withState(CREATED).build(),
                immutableTable(NATION));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testIgnorePartitionBucketingIfNotBucketed()
    {
        String tableName = mutableTablesState().get(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_1'"));
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_2'"));

        testContext().getDependency(QueryExecutor.class, "hive").executeQuery(format("ALTER TABLE %s NOT CLUSTERED", tableName));

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectStar()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());

        assertThat(query(format("SELECT * FROM %s", tableName))).matches(PRESTO_NATION_RESULT);
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(1));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(125));
    }

    @Test(groups = {HIVE_CONNECTOR},
            expectedExceptions = QueryExecutionException.class,
            expectedExceptionsMessageRegExp = ".*does not match the declared bucket count.*")
    public void testSelectAfterMultipleInsertsMultiBucketDisabled()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());

        query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAfterMultipleInserts()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());

        enableMultiFileBucketing();

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(500));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAfterMultipleInsertsForSortedTable()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_SORTED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());
        populateDataToHiveTable(tableName, NATION.getName(), Optional.empty());

        enableMultiFileBucketing();

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(500));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAfterMultipleInsertsForPartitionedTable()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_1'"));
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_2'"));
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_1'"));
        populateDataToHiveTable(tableName, NATION.getName(), Optional.of("part_key = 'insert_2'"));

        enableMultiFileBucketing();

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(4));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(20));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1 AND part_key = 'insert_1'", tableName)))
                .hasRowsCount(1)
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s WHERE part_key = 'insert_2' GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(2000));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey WHERE n.part_key = 'insert_1'", tableName, tableName)))
                .containsExactly(row(1000));
    }

    @Test(groups = {HIVE_CONNECTOR},
            expectedExceptions = QueryExecutionException.class,
            expectedExceptionsMessageRegExp = ".*\\(0\\) does not match the declared bucket count.*")
    public void testSelectFromEmptyBucketedTableEmptyTablesNotAllowed()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_EMPTY_NATION).getNameInDatabase();
        query(format("SELECT count(*) FROM %s", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectFromEmptyBucketedTableEmptyTablesAllowed()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_EMPTY_NATION).getNameInDatabase();
        enableEmptyBucketedPartitions();
        assertThat(query(format("SELECT count(*) FROM %s", tableName)))
                .containsExactly(row(0));
    }

    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectFromIncompleteBucketedTableEmptyTablesAllowed()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(BUCKETED_EMPTY_NATION).getNameInDatabase();
        enableEmptyBucketedPartitions();
        populateRowToHiveTable(tableName, ImmutableList.of("2", "'name'", "2", "'comment'"), Optional.empty());
        // insert one row into nation
        assertThat(query(format("SELECT count(*) from %s", tableName)))
                .containsExactly(row(1));
        assertThat(query(format("select n_nationkey from %s where n_regionkey = 2", tableName)))
                .containsExactly(row(2));
    }

    private static void populateRowToHiveTable(String destination, List<String> values, Optional<String> partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s" +
                        (partition.isPresent() ? format(" PARTITION (%s) ", partition.get()) : " ") +
                        "SELECT %s from (select 'foo') x",
                destination, Joiner.on(",").join(values));

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }

    private static void enableMultiFileBucketing()
            throws SQLException
    {
        setSessionProperty(defaultQueryExecutor().getConnection(), "hive.multi_file_bucketing_enabled", "true");
    }

    private static void enableEmptyBucketedPartitions()
            throws SQLException
    {
        setSessionProperty(defaultQueryExecutor().getConnection(), "hive.empty_bucketed_partitions_enabled", "true");
    }

    private static void populateDataToHiveTable(String destination, String source, Optional<String> partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s" +
                        (partition.isPresent() ? format(" PARTITION (%s) ", partition.get()) : " ") +
                        "SELECT * FROM %s",
                destination, source);

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }
}
