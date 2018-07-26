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

import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert;
import io.prestodb.tempto.query.QueryResult;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import static com.facebook.presto.tests.TemptoProductTestRunner.PRODUCT_TESTS_TIME_ZONE;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.TIMESTAMP;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static io.prestodb.tempto.util.DateTimeUtils.parseTimestampInLocalTime;
import static java.util.Collections.nCopies;

public class TestTimestampCompatibility
        extends ProductTest
{
    private static final String TABLE_NAME = "timestamp_compatibility";
    private static final String TIMESTAMP_LITERAL = "2000-01-01 07:13:55";
    private static final Timestamp EXPECTED_TIMESTAMP = parseTimestampInLocalTime(TIMESTAMP_LITERAL, PRODUCT_TESTS_TIME_ZONE);

    // format, params, legacy_mode
    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new String[][] {
                {"ORC", "true"},
                {"ORC", "false"},
                {"PARQUET", "true"},
                {"PARQUET", "false"},
                {"RCBINARY", "true"},
                {"RCBINARY", "false"},
                {"RCTEXT", "true"},
                {"RCTEXT", "false"},
                {"SEQUENCEFILE", "true"},
                {"SEQUENCEFILE", "false"},
                {"TEXTFILE", "true"},
                {"TEXTFILE", "false"}
        };
    }

    private static QueryAssert.Row singleNullRow()
    {
        return new QueryAssert.Row(new ArrayList(nCopies(1, null)));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testTimestampCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT TIMESTAMP '%s' ts", TABLE_NAME, storageFormat, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT * FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testTimestampWithNullCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT * FROM (VALUES (TIMESTAMP '%s'), (CAST(NULL AS TIMESTAMP))) t(ts)", TABLE_NAME, storageFormat, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT * FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow());
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow());
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testSingleRowArrayCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT ARRAY[TIMESTAMP '%s', TIMESTAMP '%s'] ts", TABLE_NAME, storageFormat, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[1], ts[2] FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[0], ts[1] FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP, EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP, EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNullInArrayCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT ARRAY[TIMESTAMP '%s', NULL, TIMESTAMP '%s'] ts",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[1], ts[2], ts[3] FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[0], ts[1], ts[2] FROM %s", TABLE_NAME));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP, null, EXPECTED_TIMESTAMP));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP, null, EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testMultiRowArrayCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT * FROM (VALUES(ARRAY[TIMESTAMP '%s', TIMESTAMP '%s']), (ARRAY[TIMESTAMP '%s'])) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[1] FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[0] FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP), row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP), row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNullRowInMultiRowArrayCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT * FROM (VALUES(ARRAY[TIMESTAMP '%s', TIMESTAMP '%s']), (NULL), (ARRAY[TIMESTAMP '%s'])) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT CASE WHEN ts IS NULL THEN NULL ELSE ts[1] END FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT CASE WHEN ts IS NULL THEN NULL ELSE ts[0] END FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow(), row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow(), row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNestedArrayCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT ARRAY[ARRAY[ARRAY[TIMESTAMP '%s']]] ts",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[1][1][1] FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[0][0][0] FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testSingleRowMapCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS " +
                        "SELECT * FROM " +
                        "(VALUES" +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s']))) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNullInMapCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS " +
                        "SELECT * FROM " +
                        "(VALUES" +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[CAST(NULL AS TIMESTAMP)]))) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        assertThat(hiveResult).containsExactly(singleNullRow());
        assertThat(prestoResult).containsExactly(singleNullRow());
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testMultiRowMapCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS " +
                        "SELECT * FROM " +
                        "(VALUES" +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s']))," +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s']))) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[TIMESTAMP '%s'] FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP), row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP), row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNullRowMapCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS " +
                        "SELECT * FROM " +
                        "(VALUES" +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s']))," +
                        "   (NULL)," +
                        "   (map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s']))) tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT CASE WHEN ts IS NULL THEN NULL ELSE ts[TIMESTAMP '%s'] END FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT CASE WHEN ts IS NULL THEN NULL ELSE ts[TIMESTAMP '%s'] END FROM %s", TIMESTAMP_LITERAL, TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow(), row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP), singleNullRow(), row(EXPECTED_TIMESTAMP));
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testNestedMapCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        if (storageFormat.equals("PARQUET")) {
            throw new SkipException("This is disabled due to #8729 bug in Presto.");
        }
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS " +
                        "SELECT * FROM " +
                        "(VALUES" +
                        "   (" +
                        "       map(ARRAY[TIMESTAMP '%s']," +
                        "           ARRAY[map(ARRAY[TIMESTAMP '%s']," +
                        "               ARRAY[map(ARRAY[TIMESTAMP '%s'], ARRAY[TIMESTAMP '%s'])]" +
                        "           )]" +
                        "       )" +
                        "   )" +
                        ") tst(ts)",
                TABLE_NAME, storageFormat,
                TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT ts[TIMESTAMP '%s'][TIMESTAMP '%s'][TIMESTAMP '%s'] FROM %s",
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT ts[TIMESTAMP '%s'][TIMESTAMP '%s'][TIMESTAMP '%s'] FROM %s",
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TIMESTAMP_LITERAL,
                TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP));
    }
}
