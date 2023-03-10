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

package com.getindata.flink.connector.jdbc.table;

import com.getindata.flink.connector.jdbc.JdbcTestFixture;
import com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import com.getindata.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Test suite for {@link JdbcRowDataLookupFunction}. */
public class JdbcRowDataLookupFunctionTest extends JdbcLookupTestBase {

    private static String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };

    private static String[] lookupKeys = new String[] {"id1", "id2"};

    @Test
    public void testEval() throws Exception {

        JdbcLookupOptions lookupOptions = JdbcLookupOptions.builder().build();
        JdbcRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(1, StringData.fromString("1"));

        // close connection
        lookupFunction.getDbConnection().close();

        lookupFunction.eval(2, StringData.fromString("3"));

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        Collections.sort(expected);

        assertEquals(expected, result);
    }

    @Test
    public void testEvalWithCacheMissingKeyPositive() throws Exception {

        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMissingKey(true)
                        .setCacheExpireMs(60000)
                        .setCacheMaxSize(10)
                        .build();

        JdbcRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(4, StringData.fromString("9"));
        RowData keyRow = GenericRowData.of(4, StringData.fromString("9"));

        Cache<RowData, List<RowData>> cache = lookupFunction.getCache();

        // empty data should cache
        assertEquals(cache.getIfPresent(keyRow), Collections.<RowData>emptyList());

        // put db entry for keyRow
        // final cache output should also be empty till TTL expires
        insert(
                "INSERT INTO "
                        + LOOKUP_TABLE
                        + " (id1, id2, comment1, comment2) VALUES (4, '9', '49-c1', '49-c2')");

        lookupFunction.eval(4, StringData.fromString("9"));
        assertEquals(cache.getIfPresent(keyRow), Collections.<RowData>emptyList());
    }

    @Test
    public void testEvalWithCacheMissingKeyNegative() throws Exception {

        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMissingKey(false)
                        .setCacheExpireMs(60000)
                        .setCacheMaxSize(10)
                        .build();

        JdbcRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(5, StringData.fromString("1"));
        RowData keyRow = GenericRowData.of(5, StringData.fromString("1"));

        Cache<RowData, List<RowData>> cache = lookupFunction.getCache();

        // empty data should not get cached
        assert cache.getIfPresent(keyRow) == null;

        // put db entry for keyRow
        // final cache output should contain data
        insert(
                "INSERT INTO "
                        + LOOKUP_TABLE
                        + " (id1, id2, comment1, comment2) VALUES (5, '1', '51-c1', '51-c2')");

        lookupFunction.eval(5, StringData.fromString("1"));
        List<RowData> expectedOutput = new ArrayList<>();
        expectedOutput.add(
                GenericRowData.of(
                        5,
                        StringData.fromString("1"),
                        StringData.fromString("51-c1"),
                        StringData.fromString("51-c2")));
        assertEquals(cache.getIfPresent(keyRow), expectedOutput);
    }

    private JdbcRowDataLookupFunction buildRowDataLookupFunction(JdbcLookupOptions lookupOptions) {
        com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DB_URL)
                        .setTableName(LOOKUP_TABLE)
                        .build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);

        JdbcRowDataLookupFunction lookupFunction =
                new JdbcRowDataLookupFunction(
                        jdbcOptions,
                        lookupOptions,
                        fieldNames,
                        fieldDataTypes,
                        lookupKeys,
                        rowType);

        return lookupFunction;
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
