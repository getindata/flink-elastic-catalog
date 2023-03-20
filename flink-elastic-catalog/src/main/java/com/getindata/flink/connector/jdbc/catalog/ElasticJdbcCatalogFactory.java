package com.getindata.flink.connector.jdbc.catalog;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.BASE_URL;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.PASSWORD;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.USERNAME;

public class ElasticJdbcCatalogFactory implements CatalogFactory {

    public static final ConfigOption<String> DEFAULT_SCAN_PARTITION_COLUMN_NAME =
        ConfigOptions.key("catalog.default.scan.partition.column.name")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "Catalog default scan partition column name.");

    public static final ConfigOption<Integer> DEFAULT_SCAN_PARTITION_SIZE =
        ConfigOptions.key("catalog.default.scan.partition.size")
            .intType()
            .noDefaultValue()
            .withDescription(
                "Catalog default scan partition size.");

    public static final ConfigOption<String> PROPERTIES_INDEX_PATTERNS =
        ConfigOptions.key("properties.index.patterns")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "Index patterns.");

    @Override
    public String factoryIdentifier() {
        return ElasticJdbcCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_SCAN_PARTITION_COLUMN_NAME);
        options.add(DEFAULT_SCAN_PARTITION_SIZE);
        options.add(PROPERTIES_INDEX_PATTERNS);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
            FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept("properties.scan");
        validateDynamicOptions(context.getOptions());

        return new ElasticCatalog(
            context.getName(),
            helper.getOptions().get(DEFAULT_DATABASE),
            helper.getOptions().get(USERNAME),
            helper.getOptions().get(PASSWORD),
            helper.getOptions().get(BASE_URL),
            context.getOptions()
        );
    }

    private void validateDynamicOptions(Map<String, String> options) {
        Map<String, String> scanOptions = extractScanOptions(options);
        for (Map.Entry<String, String> entry : scanOptions.entrySet()) {
            String key = entry.getKey();
            if (!(key.startsWith("properties.scan.") && key.endsWith("partition.column.name")) &&
                !(key.startsWith("properties.scan.") && key.endsWith("partition.number")) &&
                !(key.startsWith("properties.watermark.") && key.endsWith("interval")) &&
                !(key.startsWith("properties.watermark.") && key.endsWith("unit"))) {
                throw new IllegalArgumentException("Parameter " + entry.getKey() + " is not supported. We support" +
                    " properties.scan.<table_name>.partition.column.name, " +
                    " properties.scan.<table_name>.partition.number, " +
                    " properties.timeattribute.<table_name>.watermark.column, " +
                    " properties.timeattribute.<table_name>.watermark.delay, " +
                    " properties.timeattribute.<table_name>.proctime.column " +
                    "dynamic properties only."
                );
            }
        }
    }

    private Map<String, String> extractScanOptions(Map<String, String> options) {
        return options.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("properties.scan"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
