package org.apache.flink.connector.jdbc2.dialect.elastic;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ElasticTableSourceITCase extends AbstractTestBase {

    // TODO: refactor HTTP requests
    // TODO add testing for half_float and date data types

    private static final ElasticsearchTestContainer container = new ElasticsearchTestContainer();
    private static final String INPUT_TABLE_1 = "test_table_1";
    private static final String INPUT_TABLE_2 = "test_table_2";

    private static final String INPUT_PATH_1 = "elastic/single-input-event.json";
    private static final String INPUT_PATH_2 = "elastic/multiple-input-events.json";

    private static final String INDEX_PATH = "elastic/test-index.json";

    private static TableEnvironment tEnv;

    @BeforeClass
    public static void beforeAll() throws Exception {
        container.withEnv("xpack.security.enabled", "false");
        container.start();
        Class.forName(container.getDriverClassName());
        enableTrial();
        createTestIndex(INPUT_TABLE_2, INDEX_PATH);
        createTestIndex(INPUT_TABLE_1, INDEX_PATH);
        addTestData(INPUT_TABLE_2, INPUT_PATH_2);
        addTestData(INPUT_TABLE_1, INPUT_PATH_1);
    }

    private static void enableTrial() throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/_license/start_trial?acknowledge=true",
                container.getHost(), container.getElasticPort()))
            .post(RequestBody.create(null, new byte[]{}))
            .build();
        client.newCall(request).execute();
    }

    private static void createTestIndex(String inputTable, String indexPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/%s/", container.getHost(),
                container.getElasticPort(), inputTable))
            .put(RequestBody.create(loadResource(indexPath)))
            .addHeader("Content-Type", "application/json")
            .build();
        client.newCall(request).execute();
    }

    private static void addTestData(String inputTable, String inputPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/%s/_bulk/", container.getHost(),
                container.getElasticPort(), inputTable))
            .post(RequestBody.create(loadResource(inputPath)))
            .addHeader("Content-Type", "application/json")
            .build();
        client.newCall(request).execute();
    }

    private static byte[] loadResource(String path) throws IOException {
        return IOUtils.toByteArray(
            ElasticTableSourceITCase.class.getClassLoader().getResourceAsStream(path)
        );
    }

    private static void createNonPartitionedTable(String tableName) {
        tEnv.getConfig().set("table.local-time-zone", "Etc/UTC");

        String sqlString =
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "float_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "byte_col TINYINT,"
                        + "short_col SMALLINT,"
                        + "integer_col INT,"
                        + "long_col BIGINT,"
                        + "unsigned_long_col BIGINT,"
                        + "scaled_float_col DOUBLE,"
                        + "keyword_col VARCHAR,"
                        + "constant_keyword_col VARCHAR,"
                        + "wildcard_col VARCHAR,"
                        + "binary_col VARCHAR," // binary type has to be declared as varchar
                        + "date_col TIMESTAMP,"
                        + "ip_col VARCHAR,"
                        + "version_col VARCHAR,"
                        + "text_col VARCHAR,"
                        + "boolean_col BOOLEAN,"
                        + "text_multifield_col VARCHAR"
                        + ") WITH ("
                        + "  'connector'='jdbc2',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + "  'table-name'='" + tableName + "')";

        tEnv.executeSql(sqlString);
    }

    private static void createPartitionedTable(String tableName) {
        tEnv.getConfig().set("table.local-time-zone", "Etc/UTC");

        String sqlString =
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "float_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "byte_col TINYINT,"
                        + "short_col SMALLINT,"
                        + "integer_col INT,"
                        + "long_col BIGINT,"
                        + "unsigned_long_col BIGINT,"
                        + "scaled_float_col DOUBLE,"
                        + "keyword_col VARCHAR,"
                        + "constant_keyword_col VARCHAR,"
                        + "wildcard_col VARCHAR,"
                        + "binary_col VARCHAR," // binary type has to be declared as varchar
                        + "date_col TIMESTAMP,"
                        + "ip_col VARCHAR,"
                        + "version_col VARCHAR,"
                        + "text_col VARCHAR,"
                        + "boolean_col BOOLEAN,"
                        + "text_multifield_col VARCHAR"
                        + ") WITH ("
                        + "  'connector'='jdbc2',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + "  'table-name'='" + tableName + "',"
                        + "  'scan.partition.column'='integer_col',"
                        + "  'scan.partition.num'='5',"
                        + "  'scan.partition.lower-bound'='-456781',"
                        + "  'scan.partition.upper-bound'='500000')";

        tEnv.executeSql(sqlString);
    }

    private static List<String> collectResults(Iterator<Row> queryResults) {
        return CollectionUtil.iteratorToList(queryResults).stream()
                        .map(ElasticTableSourceITCase::convertTimeToLocalTime)
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
    }

    private static Row convertTimeToLocalTime(Row element) {
        LocalDateTime ldt = (LocalDateTime) element.getField("date_col");
        ZonedDateTime ldtZoned = ldt.atZone(ZoneId.systemDefault());
        ZonedDateTime utcZoned = ldtZoned.withZoneSameInstant(ZoneId.of("UTC"));
        element.setField("date_col", utcZoned.toLocalDateTime());
        return element;
    }

    @AfterClass
    public static void afterAll() throws Exception {
        Class.forName(container.getDriverClassName());
        container.stop();
    }

    @Before
    public void before() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testSelectJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_1);

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_1).collect();
        List<String> result = collectResults(collected);
        List<String> expected = Collections.singletonList("+I[1.1234, 2.123456787, 123, 12345, 1, 123123123, 123123123, 12.123, flink test, flink, flink_*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T12:10:30, 192.168.1.1, 1.2.3, aaa bbb ccc ddd eee , true, aaa]");

        assertEquals(expected, result);
    }

    @Test
    public void testFilterFloatJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> floatFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE float_col > 5 AND float_col < 6 ").collect();
        List<String> floatFilteredResult = collectResults(floatFiltered);
        List<String> expectedFloatFiltered = Collections.singletonList("+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii , false, mmm]");

        assertEquals(expectedFloatFiltered, floatFilteredResult);
    }

    @Test
    public void testFilterDoubleJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> doubleFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE double_col > 5 AND double_col < 6 ").collect();
        List<String> doubleFilteredResult = collectResults(doubleFiltered);
        List<String> expectedDoubleFiltered = Collections.singletonList("+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii , false, mmm]");

        assertEquals(expectedDoubleFiltered, doubleFilteredResult);
    }

    @Test
    public void testFilterByteJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> byteFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE byte_col > 30 AND byte_col < 40 ").collect();
        List<String> byteFilteredResult = collectResults(byteFiltered);
        List<String> expectedByteFiltered = Collections.singletonList("+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii , false, mmm]");

        assertEquals(expectedByteFiltered, byteFilteredResult);
    }

    @Test
    public void testFilterShortJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> shortFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE short_col > 15000 AND short_col < 16000 ").collect();
        List<String> shortFilteredResult = collectResults(shortFiltered);
        List<String> expectedShortFiltered = Collections.singletonList("+I[8.1234, 8.123456787, 55, 15345, -80000, 0, 2100000000, 421.823, flink test 8, flink 1, flink_8*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T11:20:10, 192.168.2.1, 1.3.1, aaa bbb ccc jjj eee , true, www]");

        assertEquals(expectedShortFiltered, shortFilteredResult);
    }

    @Test
    public void testFilterIntegerJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> integerFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE integer_col > 150000 AND integer_col < 250000 ").collect();
        List<String> integerFilteredResult = collectResults(integerFiltered);
        List<String> expectedIntegerFiltered = Collections.singletonList("+I[12.1234, 12.123456787, 90, 23345, 200000, 1200000000, 3300000000, 856.835, flink test 12, flink 1, flink_12*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T11:30:10, 192.168.3.1, 1.3.5, aaa nnn ccc ddd eee , false, 6ad]");

        assertEquals(expectedIntegerFiltered, integerFilteredResult);
    }

    @Test
    public void testFilterLongJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> longFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE long_col > 1000000000 AND long_col < 1300000000 ").collect();
        List<String> longFilteredResult = collectResults(longFiltered);
        List<String> expectedLongFiltered = Collections.singletonList("+I[12.1234, 12.123456787, 90, 23345, 200000, 1200000000, 3300000000, 856.835, flink test 12, flink 1, flink_12*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T11:30:10, 192.168.3.1, 1.3.5, aaa nnn ccc ddd eee , false, 6ad]");

        assertEquals(expectedLongFiltered, longFilteredResult);
    }

    @Test
    public void testFilterUnsignedLongJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> unsignedLongFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE unsigned_long_col > 1700000000 AND unsigned_long_col < 1900000000 ").collect();
        List<String> unsignedLongFilteredResult = collectResults(unsignedLongFiltered);
        List<String> expectedUnsignedLongFiltered = Collections.singletonList("+I[7.1234, 7.123456787, 48, 13345, -120000, -300000000, 1800000000, 412.723, flink test 7, flink 1, flink_7*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T08:50:10, 192.168.1.7, 1.3.0, aaa bbb ccc iii eee , true, sss]");

        assertEquals(expectedUnsignedLongFiltered, unsignedLongFilteredResult);
    }

    @Test
    public void testFilterScaledFloatJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> scaledFloatFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE scaled_float_col > 412.000 AND scaled_float_col < 412.500 ").collect();
        List<String> scaledFloatFilteredResult = collectResults(scaledFloatFiltered);
        List<String> expectedScaledFloatFiltered = Collections.singletonList("+I[10.1234, 10.123456787, 72, 18345, 0, 600000000, 2700000000, 412.193, flink test 10, flink 1, flink_10*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T05:10:10, 192.168.2.3, 1.3.3, aaa lll ccc ddd eee , false, l5m]");

        assertEquals(expectedScaledFloatFiltered, scaledFloatFilteredResult);
    }

    @Test
    public void testFilterKeywordJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> keywordFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE keyword_col = 'flink test 14' ").collect();
        List<String> keywordFilteredResult = collectResults(keywordFiltered);
        List<String> expectedKeywordFiltered = Collections.singletonList("+I[14.1234, 14.123456787, 103, 29345, 400000, 1800000000, 3900000000, 698.024, flink test 14, flink 1, flink_14*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T17:50:10, 192.168.3.3, 1.4.0, ppp bbb ccc ddd eee , false, zxcvf]");

        assertEquals(expectedKeywordFiltered, keywordFilteredResult);
    }

    @Test
    public void testFilterConstantKeywordJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> constantKeywordFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE constant_keyword_col = 'flink 1' ").collect();
        List<String> constantKeywordFilteredResult = collectResults(constantKeywordFiltered);

        assertEquals(15, constantKeywordFilteredResult.size());
    }

    @Test
    public void testFilterWildcardJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> wildcardFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE wildcard_col LIKE 'flink_1%' ").collect();
        List<String> wildcardFilteredResult = collectResults(wildcardFiltered);

        assertEquals(7, wildcardFilteredResult.size());
    }

    @Test
    public void testFilterIpJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> ipFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE ip_col LIKE '192.168.2.%' ").collect();
        List<String> ipFilteredResult = collectResults(ipFiltered);

        assertEquals(4, ipFilteredResult.size());
    }

    @Test
    public void testFilterVersionJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> versionFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE version_col >= '1.3.4' AND version_col < '2.0.0' ").collect();
        List<String> versionFilteredResult = collectResults(versionFiltered);

        assertEquals(4, versionFilteredResult.size());
    }

    @Test
    public void testFilterTextJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> textFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE text_col >= 'aaa nnn ccc ddd eee ' AND text_col <= 'ppp bbb ccc ddd eee ' ").collect();
        List<String> textFilteredResult = collectResults(textFiltered);

        assertEquals(2, textFilteredResult.size());
    }

    @Test
    public void testFilterBooleanJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> booleanFiltered = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE boolean_col ").collect();
        List<String> booleanFilteredResult = collectResults(booleanFiltered);

        assertEquals(7, booleanFilteredResult.size());
    }

    /**
     * Here is some description of the logic behind a multifield type.
     * If we select from a table with multifield column without a WHERE clause we will get a row with the first value in
     * multifield. However we can query records with WHERE clause specifying the value that should be looked for.
     * Here is the document describing the logic in more detail:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html#sql-multi-field
     */
    @Test
    public void testFilterTextMultifieldJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> textMultifieldFiltered1 = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE text_multifield_col = 'asd'").collect();
        List<String> textMultifieldFilteredResult1 = collectResults(textMultifieldFiltered1);

        Iterator<Row> textMultifieldFiltered2 = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE text_multifield_col = 'dfg'").collect();
        List<String> textMultifieldFilteredResult2 = collectResults(textMultifieldFiltered2);

        Iterator<Row> textMultifieldFiltered3 = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE text_multifield_col = 'bfdsab'").collect();
        List<String> textMultifieldFilteredResult3 = collectResults(textMultifieldFiltered3);

        List<String> expectedTextMultifieldFiltered1 = Collections.singletonList("+I[11.1234, 11.123456787, 81, 21345, 100000, 900000000, 3000000000, 678.456, flink test 11, flink 1, flink_11*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T09:20:10, 192.168.2.4, 1.3.4, aaa mmm ccc ddd eee , true, asd]");
        List<String> expectedTextMultifieldFiltered2 = Collections.singletonList("+I[11.1234, 11.123456787, 81, 21345, 100000, 900000000, 3000000000, 678.456, flink test 11, flink 1, flink_11*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T09:20:10, 192.168.2.4, 1.3.4, aaa mmm ccc ddd eee , true, dfg]");
        List<String> expectedTextMultifieldFiltered3 = Collections.singletonList("+I[13.1234, 13.123456787, 95, 26345, 300000, 1500000000, 3600000000, 950.12, flink test 13, flink 1, flink_13*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T14:40:10, 192.168.3.2, 1.3.6, ooo bbb ccc ddd eee , true, bfdsab]");

        assertEquals(expectedTextMultifieldFiltered1, textMultifieldFilteredResult1);
        assertEquals(expectedTextMultifieldFiltered2, textMultifieldFilteredResult2);
        assertEquals(expectedTextMultifieldFiltered3, textMultifieldFilteredResult3);
    }

    @Test
    public void testFilterTextMultifieldAndBooleanJdbcSource() {
        createNonPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> textMultifieldFiltered1 = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE byte_col = 41").collect();
        List<String> textMultifieldFilteredResult1 = collectResults(textMultifieldFiltered1);

        Iterator<Row> textMultifieldFiltered2 = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2 + " WHERE byte_col = 41 AND text_multifield_col = 'qqq'").collect();
        List<String> textMultifieldFilteredResult2 = collectResults(textMultifieldFiltered2);

        List<String> expectedTextMultifieldFiltered1 = Collections.singletonList("+I[6.1234, 6.123456787, 41, 11345, -150000, -600000000, 1500000000, 132.623, flink test 6, flink 1, flink_6*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T06:40:10, 192.168.1.6, 1.2.6, aaa bbb ccc hhh eee , true, ppp]");
        List<String> expectedTextMultifieldFiltered2 = Collections.singletonList("+I[6.1234, 6.123456787, 41, 11345, -150000, -600000000, 1500000000, 132.623, flink test 6, flink 1, flink_6*, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T06:40:10, 192.168.1.6, 1.2.6, aaa bbb ccc hhh eee , true, qqq]");

        assertEquals(expectedTextMultifieldFiltered1, textMultifieldFilteredResult1);
        assertEquals(expectedTextMultifieldFiltered2, textMultifieldFilteredResult2);
    }

    @Test
    public void testPartitionedScanJdbcSource() {
        createPartitionedTable(INPUT_TABLE_2);

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_2).collect();
        List<String> result = collectResults(collected);

        assertEquals(15, result.size());
    }
}
