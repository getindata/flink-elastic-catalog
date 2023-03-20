package com.getindata.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.AbstractDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticCatalogTest extends ElasticCatalogTestBase {
    private static final String INPUT_SINGLE_RECORD_TABLE = "test_single_record_table";
    private static final String INPUT_MULTIPLE_RECORDS_TABLE = "test_multiple_records_table";
    private static final String INPUT_MISSING_DATE_COL_TABLE = "test_missing_date_col_table";
    private static final String INPUT_UNSUPPORTED_DATA_TYPE_TABLE = "test_unsupported_data_type_table";
    private static final String INPUT_PARTIAL_SCHEMA_TABLE_1 = "test_partial_schema_table_1";
    private static final String INPUT_PARTIAL_SCHEMA_TABLE_2 = "test_partial_schema_table_2";
    private static final String INPUT_EMPTY_TABLE = "test_empty_table";
    private static final String INPUT_SPECIAL_CHARACTER_COLUMN_NAMES_TABLE = "test_special_character_column_names_table";

    private static final String INPUT_SINGLE_EVENT_PATH = "elastic/single-input-event.json";
    private static final String INPUT_MULTIPLE_EVENTS_PATH = "elastic/multiple-input-events.json";
    private static final String INPUT_NO_DATE_COL_EVENTS_PATH = "elastic/multiple-input-events-no-date-col.json";
    private static final String INPUT_UNSUPPORTED_DATA_TYPE_EVENTS_PATH = "elastic/single-input-unsupported-data-type-event.json";
    private static final String INPUT_PARTIAL_SCHEMA_EVENTS_PATH_1 = "elastic/multiple-input-events-partial-1.json";
    private static final String INPUT_PARTIAL_SCHEMA_EVENTS_PATH_2 = "elastic/multiple-input-events-partial-2.json";
    private static final String INPUT_NO_EVENTS_PATH = "elastic/empty-input-events.json";
    private static final String INPUT_SPECIAL_CHARACTER_COLUMN_NAMES_PATH = "elastic/single-input-event-special-character-column-names.json";

    private static final String INDEX_PATH = "elastic/test-index.json";
    private static final String MISSING_DATE_COL_INDEX_PATH = "elastic/test-missing-date-col-index.json";
    private static final String UNSUPPORTED_DATA_TYPE_INDEX_PATH = "elastic/test-unsupported-data-type-index.json";
    private static final String PARTIAL_SCHEMA_PATH_1 = "elastic/test-index-partial1.json";
    private static final String PARTIAL_SCHEMA_PATH_2 = "elastic/test-index-partial2.json";
    private static final String SPECIAL_CHARACTER_COLUMN_NAMES_INDEX_PATH = "elastic/test-special-character-column-names-index.json";


    @BeforeClass
    public static void beforeAll() throws Exception {
        ElasticCatalogTestBase.beforeAll();
        createTestIndex(INPUT_SINGLE_RECORD_TABLE, INDEX_PATH);
        createTestIndex(INPUT_MULTIPLE_RECORDS_TABLE, INDEX_PATH);
        createTestIndex(INPUT_MISSING_DATE_COL_TABLE, MISSING_DATE_COL_INDEX_PATH);
        createTestIndex(INPUT_UNSUPPORTED_DATA_TYPE_TABLE, UNSUPPORTED_DATA_TYPE_INDEX_PATH);
        createTestIndex(INPUT_PARTIAL_SCHEMA_TABLE_1, PARTIAL_SCHEMA_PATH_1);
        createTestIndex(INPUT_PARTIAL_SCHEMA_TABLE_2, PARTIAL_SCHEMA_PATH_2);
        createTestIndex(INPUT_EMPTY_TABLE, INDEX_PATH);
        createTestIndex(INPUT_SPECIAL_CHARACTER_COLUMN_NAMES_TABLE, SPECIAL_CHARACTER_COLUMN_NAMES_INDEX_PATH);
        addTestData(INPUT_SINGLE_RECORD_TABLE, INPUT_SINGLE_EVENT_PATH);
        addTestData(INPUT_MULTIPLE_RECORDS_TABLE, INPUT_MULTIPLE_EVENTS_PATH);
        addTestData(INPUT_MISSING_DATE_COL_TABLE, INPUT_NO_DATE_COL_EVENTS_PATH);
        addTestData(INPUT_UNSUPPORTED_DATA_TYPE_TABLE, INPUT_UNSUPPORTED_DATA_TYPE_EVENTS_PATH);
        addTestData(INPUT_PARTIAL_SCHEMA_TABLE_1, INPUT_PARTIAL_SCHEMA_EVENTS_PATH_1);
        addTestData(INPUT_PARTIAL_SCHEMA_TABLE_2, INPUT_PARTIAL_SCHEMA_EVENTS_PATH_2);
        addTestData(INPUT_EMPTY_TABLE, INPUT_NO_EVENTS_PATH);
        addTestData(INPUT_SPECIAL_CHARACTER_COLUMN_NAMES_TABLE, INPUT_SPECIAL_CHARACTER_COLUMN_NAMES_PATH);
    }

    @Test
    public void testListDatabases() throws DatabaseNotExistException, TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database",
                USERNAME, PASSWORD, url);

        // then
        List<String> databases = catalog.listDatabases();
        assertEquals(1, databases.size());
        assertEquals("docker-cluster", databases.get(0));
    }

    @Test
    public void testListTables() throws DatabaseNotExistException, InterruptedException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database",
                USERNAME, PASSWORD, url);

        // then
        List<String> tables = catalog.listTables("docker-cluster");
        List<String> expectedTables = new LinkedList<>();
        expectedTables.add("test_single_record_table");
        expectedTables.add("test_multiple_records_table");
        expectedTables.add("test_missing_date_col_table");
        expectedTables.add("test_unsupported_data_type_table");
        expectedTables.add("test_partial_schema_table_1");
        expectedTables.add("test_partial_schema_table_2");
        expectedTables.add("test_empty_table");
        expectedTables.add("test_special_character_column_names_table");

        assertEquals(8, tables.size());
        assertTrue(tables.containsAll(expectedTables));
    }

    @Test
    public void testTableExists() throws DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database",
                USERNAME, PASSWORD, url);

        // then
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_single_record_table")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_multiple_records_table")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_missing_date_col_table")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_unsupported_data_type_table")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_partial_schema_table_1")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_partial_schema_table_2")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_empty_table")));
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_special_character_column_names_table")));
    }

    @Test
    public void testTableNotExists() {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database",
                USERNAME, PASSWORD, url);

        // then
        assertFalse(catalog.tableExists(new ObjectPath("docker-cluster", "nonexisting_table")));
    }

    @Test
    public void testGetNonPartitionedTable() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database",
                USERNAME, PASSWORD, url);
        CatalogBaseTable table = catalog.getTable(new ObjectPath(
                "docker-cluster", "test_multiple_records_table"));

        // then
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertFalse(table.getOptions().containsKey("scan.partition.column"));
        assertFalse(table.getOptions().containsKey("scan.partition.num"));
        assertFalse(table.getOptions().containsKey("scan.partition.lower-bound"));
        assertFalse(table.getOptions().containsKey("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTablePartitionedByTimestamp() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.column.name", "date_col");
        properties.put("properties.scan.test_multiple_records_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath(
                "docker-cluster", "test_multiple_records_table"));

        // then
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("date_col", table.getOptions().get("scan.partition.column"));
        assertEquals("10", table.getOptions().get("scan.partition.num"));
        assertEquals("1420089310000", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTablePartitionedByInteger() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.column.name", "integer_col");
        properties.put("properties.scan.test_multiple_records_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath(
                "docker-cluster",
                "test_multiple_records_table"));

        // then
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("integer_col", table.getOptions().get("scan.partition.column"));
        assertEquals("10", table.getOptions().get("scan.partition.num"));
        assertEquals("-456781", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals("500000", table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTableDefaultScanOptionsZeroRecords() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("catalog.default.scan.partition.column.name", "date_col");
        properties.put("catalog.default.scan.partition.size", "100");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_empty_table"));

        // then
        String expectedLowerBound = calculateExpectedTemporalLowerBound();
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());

        assertEquals("date_col", table.getOptions().get("scan.partition.column"));
        assertEquals("1", table.getOptions().get("scan.partition.num"));
        assertEquals(expectedLowerBound, table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testFailNoPartitionColumnProvided() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Missing column.name property for table test_multiple_records_table"));
        }
    }

    @Test
    public void testFailNoPartitionNumberProvided() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.column.name", "date_col");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Missing partition.number property for table test_multiple_records_table"));
        }
    }

    @Test
    public void testFailNoPartitionColumnInTable() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_missing_date_col_table.partition.column.name", "date_col");
        properties.put("properties.scan.test_missing_date_col_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_missing_date_col_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Partition column was not found in the specified table"));
        }
    }

    @Test
    public void testFailPartitionColumnNotSupported() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_single_record_table.partition.column.name", "keyword_col");
        properties.put("properties.scan.test_single_record_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_single_record_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Partition column is of type STRING. We support only NUMERIC, DATE and TIMESTAMP partition columns."));
        }
    }

    @Test
    public void testFailInappropriatePartitionNumber() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.column.name", "date_col");
        properties.put("properties.scan.test_multiple_records_table.partition.number", "0");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Partition number has to be greater than 0!"));
        }
    }

    @Test
    public void testUnsupportedDataTypeInTable() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_unsupported_data_type_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("We do not support the data type 'GEO_POINT' for column 'geo_point_col'!"));
        }
    }

    @Test
    public void testGetTableDefaultCatalogScanPartitionProperties() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("catalog.default.scan.partition.column.name", "date_col");
        properties.put("catalog.default.scan.partition.size", "5");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

        // then
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("date_col", table.getOptions().get("scan.partition.column"));
        assertEquals("3", table.getOptions().get("scan.partition.num"));
        assertEquals("1420089310000", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTableOverwriteCatalogScanProperties() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_multiple_records_table.partition.column.name", "integer_col");
        properties.put("properties.scan.test_multiple_records_table.partition.number", "3");
        properties.put("catalog.default.scan.partition.column.name", "date_col");
        properties.put("catalog.default.scan.partition.size", "5");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

        // then
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("integer_col", table.getOptions().get("scan.partition.column"));
        assertEquals("3", table.getOptions().get("scan.partition.num"));
        assertEquals("-456781", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals("500000", table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTableIndexPattern() throws TableNotExistException, DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.index.patterns", "test_*_record_table");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_*_record_table"));

        // then
        List<String> tables = catalog.listTables("docker-cluster");
        Schema schema = table.getUnresolvedSchema();

        Schema expectedSchema = Schema.newBuilder().fromFields(
                new String[]{"binary_col", "boolean_col", "byte_col",
                        "constant_keyword_col", "date_col", "date_epoch_col",
                        "date_nanos_col", "double_col", "float_col",
                        "half_float_col", "integer_col", "ip_col",
                        "keyword_col", "long_col", "scaled_float_col",
                        "short_col", "text_col", "text_multifield_col",
                        "unsigned_long_col", "version_col", "wildcard_col"},
                new AbstractDataType[]{
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.STRING(),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.DOUBLE(),
                        DataTypes.FLOAT(),
                        DataTypes.FLOAT(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.BIGINT(),
                        DataTypes.DOUBLE(),
                        DataTypes.SMALLINT(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.STRING()
                }).build();
        assertEquals(9, tables.size());
        assertNotNull(table);
        assertFalse(table.getOptions().containsKey("scan.partition.column"));
        assertFalse(table.getOptions().containsKey("scan.partition.num"));
        assertFalse(table.getOptions().containsKey("scan.partition.lower-bound"));
        assertFalse(table.getOptions().containsKey("scan.partition.upper-bound"));
        assertEquals(expectedSchema, schema);
    }

    @Test
    public void testGetMultipleIndexPatternPartitionedTables() throws TableNotExistException, DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_*_record*_table.partition.column.name", "date_col");
        properties.put("properties.scan.test_*_record*_table.partition.number", "10");
        properties.put("properties.scan.test_partial_schema_table_*.partition.column.name", "integer_col");
        properties.put("properties.scan.test_partial_schema_table_*.partition.number", "5");
        properties.put("properties.index.patterns", "test_*_record*_table,test_partial_schema_table_*");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_*_record*_table"));
        CatalogBaseTable table2 = catalog.getTable(new ObjectPath("docker-cluster", "test_partial_schema_table_*"));

        // then
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        List<String> tables = catalog.listTables("docker-cluster");

        assertEquals(10, tables.size());

        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("date_col", table.getOptions().get("scan.partition.column"));
        assertEquals("10", table.getOptions().get("scan.partition.num"));
        assertEquals("1420089310000", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));

        assertNotNull(table2);
        assertNotNull(table2.getUnresolvedSchema());
        assertEquals("integer_col", table2.getOptions().get("scan.partition.column"));
        assertEquals("5", table2.getOptions().get("scan.partition.num"));
        assertEquals("-256781", table2.getOptions().get("scan.partition.lower-bound"));
        assertEquals("950000", table2.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testGetTableDuplicatedIndexPattern() throws TableNotExistException, DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.index.patterns", "test_partial_schema_table_*, test_partial_schema_table_*");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        catalog.getTable(new ObjectPath("docker-cluster", "test_partial_schema_table_*"));

        // then
        List<String> tables = catalog.listTables("docker-cluster");
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_partial_schema_table_*")));
        assertEquals(9, tables.size());
    }

    @Test
    public void testGetTableIndexPatternDifferentTableSchamas() throws TableNotExistException, DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_partial_schema_table_*.partition.column.name", "date_col");
        properties.put("properties.scan.test_partial_schema_table_*.partition.number", "10");
        properties.put("properties.index.patterns", "test_partial_schema_table_*");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_partial_schema_table_*"));

        Schema expectedSchema = Schema.newBuilder().fromFields(
                new String[]{"date_col", "double_col", "integer_col", "ip_col", "keyword_col", "short_col", "version_col"},
                new AbstractDataType[]{
                        DataTypes.TIMESTAMP(6),
                        DataTypes.DOUBLE(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.SMALLINT(),
                        DataTypes.STRING()}).build();
        // then
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        Schema schema = table.getUnresolvedSchema();
        List<String> tables = catalog.listTables("docker-cluster");
        assertEquals(9, tables.size());
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("date_col", table.getOptions().get("scan.partition.column"));
        assertEquals("10", table.getOptions().get("scan.partition.num"));
        assertEquals("1420607710000", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));
        assertEquals(expectedSchema, schema);
    }

    @Test
    public void testGetTablePartitionBySpecialCharacterColumn() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.scan.test_special_character_column_names_table.partition.column.name", "@timestamp");
        properties.put("properties.scan.test_special_character_column_names_table.partition.number", "10");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_special_character_column_names_table"));

        // then
        String expectedUpperBound = calculateExpectedTemporalUpperBound();
        assertNotNull(table);
        assertNotNull(table.getUnresolvedSchema());
        assertEquals("@timestamp", table.getOptions().get("scan.partition.column"));
        assertEquals("10", table.getOptions().get("scan.partition.num"));
        assertEquals("1420114230000", table.getOptions().get("scan.partition.lower-bound"));
        assertEquals(expectedUpperBound, table.getOptions().get("scan.partition.upper-bound"));
    }

    @Test
    public void testPreservesColumnsOrder() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url);

        // then
        String[] actualColumnNames = catalog.getTable(new ObjectPath("docker-cluster", "test_single_record_table"))
                .getUnresolvedSchema()
                .getColumns()
                .stream()
                .map(UnresolvedColumn::getName)
                .toArray(String[]::new);

        for (int i = 0; i < actualColumnNames.length - 1; ++i) {
            // Elastic driver returns columns in alphabetical order.
            assertTrue(actualColumnNames[i].compareTo(actualColumnNames[i + 1]) < 0);
        }
    }

    @Test
    public void testGetTableTimeAttributesProctime() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.timeattribute.test_multiple_records_table.proctime.column", "my_proctime");

        // when
        ElasticCatalog catalog = new ElasticCatalog(
                "test-catalog",
                "test-database",
                USERNAME,
                PASSWORD,
                url,
                properties
        );
        CatalogBaseTable table = catalog.getTable(new ObjectPath(
                "docker-cluster",
                "test_multiple_records_table"
        ));

        // then
        Schema schema = table.getUnresolvedSchema();
        assertNotNull(table);
        assertNotNull(schema);

        List<UnresolvedColumn> columns = schema.getColumns();
        UnresolvedColumn actualColumn = columns.get(columns.size() - 1);
        UnresolvedComputedColumn expectedColumn = (UnresolvedComputedColumn) Schema.newBuilder().columnByExpression(
                "my_proctime",
                "PROCTIME()"
        ).build().getColumns().get(0);
        assertEquals(actualColumn, expectedColumn);
    }

    @Test
    public void testGetTableTimeAttributesWatermark() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.timeattribute.test_multiple_records_table.watermark.column", "date_col");
        properties.put("properties.timeattribute.test_multiple_records_table.watermark.delay", "'5' SECOND");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        CatalogBaseTable table = catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

        // then
        Schema schema = table.getUnresolvedSchema();

        assertEquals(1, schema.getWatermarkSpecs().size());
        assertEquals("[date_col - INTERVAL '5' SECOND]", schema.getWatermarkSpecs().get(0).getWatermarkExpression().toString());
    }

    @Test
    public void testFailGetTableTimeAttributesProctimeAndWatermarkProvided() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.timeattribute.test_multiple_records_table.proctime.column", "my_proctime");
        properties.put("properties.timeattribute.test_multiple_records_table.watermark.column", "date_col");
        properties.put("properties.timeattribute.test_multiple_records_table.watermark.delay", "'5' SECOND");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("Either proctime or watermark properties should be specified for a table test_multiple_records_table."));
        }
    }

    @Test
    public void testFailGetTableWithWatermarkMissingProperty() throws TableNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("properties.timeattribute.test_multiple_records_table.watermark.column", "date_col");

        // when
        ElasticCatalog catalog = new ElasticCatalog("test-catalog", "test-database", USERNAME,
                PASSWORD, url, properties);
        try {
            catalog.getTable(new ObjectPath("docker-cluster", "test_multiple_records_table"));

            // then
            fail("Should have thrown CatalogException");
        } catch (CatalogException e) {
            assertTrue(e.getCause().getMessage().contains("You should specify both watermarkDelay and watermarkColumn properties when using watermark for a table test_multiple_records_table."));
        }
    }
}
