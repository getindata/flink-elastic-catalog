package com.getindata.flink.connector.jdbc.catalog.factory;

import com.getindata.flink.connector.jdbc.catalog.ElasticCatalog;
import com.getindata.flink.connector.jdbc.catalog.ElasticsearchTestContainer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.factories.CatalogFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticJdbcCatalogFactoryTest {
    private static ElasticJdbcCatalogFactory catalogFactory;
    private static String url;
    private static final ElasticsearchTestContainer container = new ElasticsearchTestContainer();

    @BeforeClass
    public static void setup() {
        container.withEnv("xpack.security.enabled", "false");
        container.start();

        catalogFactory = new ElasticJdbcCatalogFactory();

        url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
                container.getElasticPort());
    }

    @Test
    public void testCreateElasticCatalogNoAdditionalOptions() {
        // given
        Map<String, String> options = getCommonOptions();
        Context catalogContext = new FactoryUtil.DefaultCatalogContext(
                "test-catalog",
                options,
                new Configuration(),
                ElasticJdbcCatalogFactoryTest.class.getClassLoader());

        // when
        ElasticCatalog catalog = (ElasticCatalog) catalogFactory.createCatalog(catalogContext);

        // then
        List<String> databases = catalog.listDatabases();
        assertTrue(catalog.getBaseUrl().startsWith("jdbc:elasticsearch://localhost:"));
        assertEquals(1, databases.size());
        assertEquals("docker-cluster", databases.get(0));
    }

    @Test
    public void testCreateElasticCatalogTableScanPartitionOptions() {
        // given
        Map<String, String> options = getCommonOptions();
        options.put("properties.scan.example-table.partition.column.name", "record_time");
        options.put("properties.scan.example-table.partition.number", "20");

        Context catalogContext = new FactoryUtil.DefaultCatalogContext(
                "test-catalog",
                options,
                new Configuration(),
                ElasticJdbcCatalogFactoryTest.class.getClassLoader());

        // when
        ElasticCatalog catalog = (ElasticCatalog) catalogFactory.createCatalog(catalogContext);

        // then
        Map<String, ElasticCatalog.ScanPartitionProperties> scanPartitionProperties = catalog.getScanPartitionProperties();

        assertTrue(scanPartitionProperties.containsKey("example-table"));
        assertEquals("record_time", scanPartitionProperties.get("example-table").getPartitionColumnName());
        assertEquals(Integer.valueOf(20), scanPartitionProperties.get("example-table").getPartitionNumber());
    }

    @Test
    public void testCreateElasticCatalogDefaultPartitionOptions() {
        // given
        Map<String, String> options = getCommonOptions();
        options.put("catalog.default.scan.partition.column.name", "record_time");
        options.put("catalog.default.scan.partition.size", "1000");

        Context catalogContext = new FactoryUtil.DefaultCatalogContext("test-catalog",
                options,
                new Configuration(),
                ElasticJdbcCatalogFactoryTest.class.getClassLoader());

        // when
        ElasticCatalog catalog = (ElasticCatalog) catalogFactory.createCatalog(catalogContext);

        // then
        assertEquals("record_time", catalog.getCatalogDefaultScanPartitionColumnName());
        assertEquals("1000", catalog.getCatalogDefaultScanPartitionCapacity());
    }

    @Test
    public void testCreateElasticCatalogIndexPatternsOptions() throws DatabaseNotExistException {
        // given
        Map<String, String> options = getCommonOptions();
        options.put("properties.index.patterns", "example_table_*");

        Context catalogContext = new FactoryUtil.DefaultCatalogContext("test-catalog",
                options,
                new Configuration(),
                ElasticJdbcCatalogFactoryTest.class.getClassLoader());

        // when
        ElasticCatalog catalog = (ElasticCatalog) catalogFactory.createCatalog(catalogContext);

        // then
        assertEquals(singletonList("example_table_*"), catalog.getIndexPatterns());
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "example_table_*")));
    }

    private Map<String, String> getCommonOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("base-url", url);
        options.put("default-database", "test-database");
        options.put("password", "password");
        options.put("username", "user");
        return options;
    }
}
