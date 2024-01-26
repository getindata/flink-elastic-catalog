/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.flink.connector.jdbc.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.ADD_PROCTIME_COLUMN;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.BASE_URL;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.DEFAULT_SCAN_PARTITION_COLUMN_NAME;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.DEFAULT_SCAN_PARTITION_SIZE;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.EXCLUDE;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.INCLUDE;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.PASSWORD;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.PROPERTIES_INDEX_PATTERNS;
import static com.getindata.flink.connector.jdbc.catalog.ElasticJdbcCatalogFactoryOptions.USERNAME;

public class ElasticJdbcCatalogFactory implements CatalogFactory {

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
        options.add(ADD_PROCTIME_COLUMN);
        options.add(EXCLUDE);
        options.add(INCLUDE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept("properties.scan");
        validateDynamicOptions(context.getOptions());

        return new ElasticCatalog(
                context.getClassLoader(),
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(BASE_URL),
                Boolean.getBoolean(context.getOptions().get(ADD_PROCTIME_COLUMN)),
                IndexFilterResolver.of(helper.getOptions().get(INCLUDE), helper.getOptions().get(EXCLUDE)),
                context.getOptions()
        );
    }

    private void validateDynamicOptions(Map<String, String> options) {
        Map<String, String> scanOptions = extractScanOptions(options);
        for (Map.Entry<String, String> entry : scanOptions.entrySet()) {
            String key = entry.getKey();
            if (!(key.startsWith("properties.scan.") && key.endsWith("partition.column.name")) &&
                    !(key.startsWith("properties.scan.") && key.endsWith("partition.number"))) {
                throw new IllegalArgumentException("Parameter " + entry.getKey() + " is not supported." +
                        " We support only the following dynamic properties:\n" +
                        " properties.scan.<table_name>.partition.column.name\n" +
                        " properties.scan.<table_name>.partition.number"
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
