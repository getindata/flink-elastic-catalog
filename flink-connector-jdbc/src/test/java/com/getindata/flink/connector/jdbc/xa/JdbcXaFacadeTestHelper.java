/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.flink.connector.jdbc.xa;

import com.getindata.flink.connector.jdbc.JdbcTestCheckpoint;
import com.getindata.flink.connector.jdbc.JdbcTestFixture;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

class JdbcXaFacadeTestHelper implements AutoCloseable {
    private final String table;
    private final String dbUrl;
    private final String user;
    private final String pass;
    private final XaFacade xaFacade;

    JdbcXaFacadeTestHelper(
            XADataSource xaDataSource, String dbUrl, String table, String user, String pass)
            throws Exception {
        this.dbUrl = dbUrl;
        this.table = table;
        this.xaFacade = XaFacadeImpl.fromXaDataSource(xaDataSource);
        this.xaFacade.open();
        this.user = user;
        this.pass = pass;
    }

    void assertPreparedTxCountEquals(int expected) {
        assertEquals(expected, xaFacade.recover().size());
    }

    void assertDbContentsEquals(JdbcTestCheckpoint... checkpoints) throws SQLException {
        assertDbContentsEquals(
                Arrays.stream(checkpoints).flatMapToInt(x -> Arrays.stream(x.dataItemsIdx)));
    }

    void assertDbContentsEquals(IntStream dataIdxStream) throws SQLException {
        assertDbContentsEquals(
                dataIdxStream.map(idx -> JdbcTestFixture.TEST_DATA[idx].id).boxed().collect(Collectors.toList()));
    }

    void assertDbContentsEquals(List<Integer> expected) throws SQLException {
        assertEquals(expected, getInsertedIds());
    }

    private List<Integer> getInsertedIds() throws SQLException {
        return getInsertedIds(dbUrl, user, pass, table);
    }

    static List<Integer> getInsertedIds(String dbUrl, String user, String pass, String table)
            throws SQLException {
        List<Integer> dbContents = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(dbUrl, user, pass)) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select id from " + table)) {
                    while (rs.next()) {
                        dbContents.add(rs.getInt(1));
                    }
                }
            }
        }
        return dbContents;
    }

    int countInDb() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select count(1) from " + table)) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        xaFacade.close();
    }

    void cancelAllTx() {
        xaFacade.recover().forEach(xaFacade::rollback);
    }
}
