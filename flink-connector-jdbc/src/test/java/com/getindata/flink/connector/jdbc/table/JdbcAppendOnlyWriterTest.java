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

import com.getindata.flink.connector.jdbc.DbMetadata;
import com.getindata.flink.connector.jdbc.JdbcDataTestBase;
import com.getindata.flink.connector.jdbc.JdbcTestBase;
import com.getindata.flink.connector.jdbc.JdbcTestFixture;
import com.getindata.flink.connector.jdbc.internal.JdbcOutputFormat;
import com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.mockito.Mockito.doReturn;

/** Test for the Append only mode. */
public class JdbcAppendOnlyWriterTest extends JdbcTestBase {

    private JdbcOutputFormat format;
    private String[] fieldNames;

    @Before
    public void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    }

    @Test(expected = IOException.class)
    public void testMaxRetry() throws Exception {
        format =
                JdbcOutputFormat.builder()
                        .setOptions(
                                JdbcConnectorOptions.builder()
                                        .setDBUrl(getDbMetadata().getUrl())
                                        .setTableName(JdbcTestFixture.OUTPUT_TABLE)
                                        .build())
                        .setFieldNames(fieldNames)
                        .setKeyFields(null)
                        .build();
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
        doReturn(config).when(context).getExecutionConfig();
        doReturn(true).when(config).isObjectReuseEnabled();
        format.setRuntimeContext(context);
        format.open(0, 1);

        // alter table schema to trigger retry logic after failure.
        alterTable();
        for (JdbcTestFixture.TestEntry entry : JdbcTestFixture.TEST_DATA) {
            format.writeRecord(Tuple2.of(true, JdbcDataTestBase.toRow(entry)));
        }

        // after retry default times, throws a BatchUpdateException.
        format.flush();
    }

    private void alterTable() throws Exception {
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("ALTER  TABLE " + JdbcTestFixture.OUTPUT_TABLE + " DROP COLUMN " + fieldNames[1]);
        }
    }

    @After
    public void clear() throws Exception {
        if (format != null) {
            try {
                format.close();
            } catch (RuntimeException e) {
                // ignore exception when close.
            }
        }
        format = null;
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + JdbcTestFixture.OUTPUT_TABLE);
        }
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return JdbcTestFixture.DERBY_EBOOKSHOP_DB;
    }
}
