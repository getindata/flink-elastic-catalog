package com.getindata.flink.connector.jdbc.catalog.factory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

@Internal
public class ElasticJdbcCatalogFactoryOptions {
    public static final String IDENTIFIER = "elasticsearch";

    public static final ConfigOption<String> DEFAULT_DATABASE =
        ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Default database name to use.");

    public static final ConfigOption<String> USERNAME =
        ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("ElasticSearch Username to use.");

    public static final ConfigOption<String> PASSWORD =
        ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("ElasticSearch Password to use.");

    public static final ConfigOption<String> BASE_URL =
        ConfigOptions.key("base-url")
            .stringType()
            .noDefaultValue()
            .withDescription("Url of ElasticSearch.");

    private ElasticJdbcCatalogFactoryOptions() {
    }
}
