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

import com.getindata.flink.connector.jdbc.JdbcDataTestBase;
import com.getindata.flink.connector.jdbc.JdbcTestFixture;
import com.getindata.flink.connector.jdbc.internal.JdbcOutputFormat;
import com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import com.getindata.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import com.getindata.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test suite for {@link JdbcOutputFormatBuilder}. */
public class JdbcOutputFormatTest extends JdbcDataTestBase {

    private static JdbcOutputFormat<RowData, ?, ?> outputFormat;
    private static String[] fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.DOUBLE(),
                DataTypes.INT()
            };
    private static RowType rowType =
            RowType.of(
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new),
                    fieldNames);
    private static InternalTypeInfo<RowData> rowDataTypeInfo = InternalTypeInfo.of(rowType);

    @After
    public void tearDown() throws Exception {
        if (outputFormat != null) {
            outputFormat.close();
        }
        outputFormat = null;
    }

    @Test
    public void testInvalidDriver() {
        String expectedMsg = "unable to open JDBC writer";
        try {
            com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                    com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                            .setDriverName("org.apache.derby.jdbc.idontexist")
                            .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                            .setTableName(JdbcTestFixture.INPUT_TABLE)
                            .build();
            JdbcDmlOptions dmlOptions =
                    JdbcDmlOptions.builder()
                            .withTableName(jdbcOptions.getTableName())
                            .withDialect(jdbcOptions.getDialect())
                            .withFieldNames(fieldNames)
                            .build();

            outputFormat =
                    new JdbcOutputFormatBuilder()
                            .setJdbcOptions(jdbcOptions)
                            .setFieldDataTypes(fieldDataTypes)
                            .setJdbcDmlOptions(dmlOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .build();
            outputFormat.open(0, 1);
            fail("Expected exception is not thrown.");
        } catch (Exception e) {
            assertTrue(findThrowable(e, IOException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testInvalidURL() {
        try {
            com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                    com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                            .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                            .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                            .setTableName(JdbcTestFixture.INPUT_TABLE)
                            .build();
            JdbcDmlOptions dmlOptions =
                    JdbcDmlOptions.builder()
                            .withTableName(jdbcOptions.getTableName())
                            .withDialect(jdbcOptions.getDialect())
                            .withFieldNames(fieldNames)
                            .build();

            outputFormat =
                    new JdbcOutputFormatBuilder()
                            .setJdbcOptions(jdbcOptions)
                            .setFieldDataTypes(fieldDataTypes)
                            .setJdbcDmlOptions(dmlOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .build();
            outputFormat.open(0, 1);
            fail("Expected exception is not thrown.");
        } catch (Exception e) {
            assertTrue(findThrowable(e, IllegalStateException.class).isPresent());
        }
    }

    @Test
    public void testIncompatibleTypes() {
        try {
            com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                    com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                            .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                            .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                            .setTableName(JdbcTestFixture.INPUT_TABLE)
                            .build();
            JdbcDmlOptions dmlOptions =
                    JdbcDmlOptions.builder()
                            .withTableName(jdbcOptions.getTableName())
                            .withDialect(jdbcOptions.getDialect())
                            .withFieldNames(fieldNames)
                            .build();

            outputFormat =
                    new JdbcOutputFormatBuilder()
                            .setJdbcOptions(jdbcOptions)
                            .setFieldDataTypes(fieldDataTypes)
                            .setJdbcDmlOptions(dmlOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .setRowDataTypeInfo(rowDataTypeInfo)
                            .build();

            setRuntimeContext(outputFormat, false);
            outputFormat.open(0, 1);

            RowData row = buildGenericData(4, "hello", "world", 0.99, "imthewrongtype");
            outputFormat.writeRecord(row);
            outputFormat.close();
            fail("Expected exception is not thrown.");
        } catch (Exception e) {
            assertTrue(findThrowable(e, ClassCastException.class).isPresent());
        }
    }

    @Test
    public void testExceptionOnInvalidType() {
        try {
            com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                    com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                            .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                            .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                            .setTableName(JdbcTestFixture.OUTPUT_TABLE)
                            .build();
            JdbcDmlOptions dmlOptions =
                    JdbcDmlOptions.builder()
                            .withTableName(jdbcOptions.getTableName())
                            .withDialect(jdbcOptions.getDialect())
                            .withFieldNames(fieldNames)
                            .build();

            outputFormat =
                    new JdbcOutputFormatBuilder()
                            .setJdbcOptions(jdbcOptions)
                            .setFieldDataTypes(fieldDataTypes)
                            .setJdbcDmlOptions(dmlOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .setRowDataTypeInfo(rowDataTypeInfo)
                            .build();
            setRuntimeContext(outputFormat, false);
            outputFormat.open(0, 1);

            JdbcTestFixture.TestEntry entry = JdbcTestFixture.TEST_DATA[0];
            RowData row = buildGenericData(entry.id, entry.title, entry.author, 0L, entry.qty);
            outputFormat.writeRecord(row);
            outputFormat.close();
            fail("Expected exception is not thrown.");
        } catch (Exception e) {
            assertTrue(findThrowable(e, ClassCastException.class).isPresent());
        }
    }

    @Test
    public void testExceptionOnClose() {
        String expectedMsg = "Writing records to JDBC failed.";
        try {
            com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                    com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                            .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                            .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                            .setTableName(JdbcTestFixture.OUTPUT_TABLE)
                            .build();
            JdbcDmlOptions dmlOptions =
                    JdbcDmlOptions.builder()
                            .withTableName(jdbcOptions.getTableName())
                            .withDialect(jdbcOptions.getDialect())
                            .withFieldNames(fieldNames)
                            .build();

            outputFormat =
                    new JdbcOutputFormatBuilder()
                            .setJdbcOptions(jdbcOptions)
                            .setFieldDataTypes(fieldDataTypes)
                            .setJdbcDmlOptions(dmlOptions)
                            .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                            .setRowDataTypeInfo(rowDataTypeInfo)
                            .build();
            setRuntimeContext(outputFormat, true);
            outputFormat.open(0, 1);

            JdbcTestFixture.TestEntry entry = JdbcTestFixture.TEST_DATA[0];
            RowData row =
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty);

            outputFormat.writeRecord(row);
            outputFormat.writeRecord(
                    row); // writing the same record twice must yield a unique key violation.
            outputFormat.close();

            fail("Expected exception is not thrown.");
        } catch (Exception e) {
            assertTrue(findThrowable(e, RuntimeException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testJdbcOutputFormat() throws IOException, SQLException {
        com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                        .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(JdbcTestFixture.OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        for (JdbcTestFixture.TestEntry entry : JdbcTestFixture.TEST_DATA) {
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl());
             PreparedStatement statement = dbConn.prepareStatement(JdbcTestFixture.SELECT_ALL_NEWBOOKS);
             ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].id, resultSet.getObject("id"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].title, resultSet.getObject("title"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].author, resultSet.getObject("author"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].price, resultSet.getObject("price"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

                recordCount++;
            }
            Assert.assertEquals(JdbcTestFixture.TEST_DATA.length, recordCount);
        }
    }

    @Test
    public void testFlush() throws SQLException, IOException {
        com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                        .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(JdbcTestFixture.OUTPUT_TABLE_2)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(3).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        try (Connection dbConn = DriverManager.getConnection(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl());
             PreparedStatement statement = dbConn.prepareStatement(JdbcTestFixture.SELECT_ALL_NEWBOOKS_2)) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                JdbcTestFixture.TEST_DATA[i].id,
                                JdbcTestFixture.TEST_DATA[i].title,
                                JdbcTestFixture.TEST_DATA[i].author,
                                JdbcTestFixture.TEST_DATA[i].price,
                                JdbcTestFixture.TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertFalse(resultSet.next());
            }
            outputFormat.writeRecord(
                    buildGenericData(
                            JdbcTestFixture.TEST_DATA[2].id,
                            JdbcTestFixture.TEST_DATA[2].title,
                            JdbcTestFixture.TEST_DATA[2].author,
                            JdbcTestFixture.TEST_DATA[2].price,
                            JdbcTestFixture.TEST_DATA[2].qty));
            try (ResultSet resultSet = statement.executeQuery()) {
                int recordCount = 0;
                while (resultSet.next()) {
                    Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].id, resultSet.getObject("id"));
                    Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].title, resultSet.getObject("title"));
                    Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].author, resultSet.getObject("author"));
                    Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].price, resultSet.getObject("price"));
                    Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].qty, resultSet.getObject("qty"));
                    recordCount++;
                }
                assertEquals(3, recordCount);
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    public void testFlushWithBatchSizeEqualsZero() throws SQLException, IOException {
        com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions.builder()
                        .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(JdbcTestFixture.OUTPUT_TABLE_2)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(0).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);

        try (Connection dbConn = DriverManager.getConnection(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl());
             PreparedStatement statement = dbConn.prepareStatement(JdbcTestFixture.SELECT_ALL_NEWBOOKS_2)) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                JdbcTestFixture.TEST_DATA[i].id,
                                JdbcTestFixture.TEST_DATA[i].title,
                                JdbcTestFixture.TEST_DATA[i].author,
                                JdbcTestFixture.TEST_DATA[i].price,
                                JdbcTestFixture.TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertFalse(resultSet.next());
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    public void testInvalidConnectionInJdbcOutputFormat() throws IOException, SQLException {
        com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(JdbcTestFixture.OUTPUT_TABLE_3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        // write records
        for (int i = 0; i < 3; i++) {
            JdbcTestFixture.TestEntry entry = JdbcTestFixture.TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        // close connection
        outputFormat.getConnection().close();

        // continue to write rest records
        for (int i = 3; i < JdbcTestFixture.TEST_DATA.length; i++) {
            JdbcTestFixture.TestEntry entry = JdbcTestFixture.TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl());
             PreparedStatement statement = dbConn.prepareStatement(JdbcTestFixture.SELECT_ALL_NEWBOOKS_3);
             ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].id, resultSet.getObject("id"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].title, resultSet.getObject("title"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].author, resultSet.getObject("author"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].price, resultSet.getObject("price"));
                Assert.assertEquals(JdbcTestFixture.TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

                recordCount++;
            }
            Assert.assertEquals(JdbcTestFixture.TEST_DATA.length, recordCount);
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getDriverClass());
        try (Connection conn = DriverManager.getConnection(JdbcTestFixture.DERBY_EBOOKSHOP_DB.getUrl());
             Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + JdbcTestFixture.OUTPUT_TABLE);
        }
    }
}
