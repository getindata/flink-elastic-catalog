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

package com.getindata.flink.connector.jdbc.dialect.oracle;

import com.getindata.flink.connector.jdbc.dialect.JdbcDialect;
import com.getindata.flink.connector.jdbc.dialect.JdbcDialectLoader;
import com.getindata.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/** Tests for {@link OraclePreparedStatementTest}. */
public class OraclePreparedStatementTest {

    private final JdbcDialect dialect = JdbcDialectLoader.load("jdbc:oracle://localhost:3306/test");
    private final String[] fieldNames =
            new String[] {"id", "name", "email", "ts", "field1", "field_2", "__field_3__"};
    private final String[] keyFields = new String[] {"id", "__field_3__"};
    private final String tableName = "tbl";

    @Test
    public void testInsertStatement() {
        String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        assertEquals(
                "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) "
                        + "VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__)",
                insertStmt);
        NamedStatementMatcher.parsedSql(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?)")
                .parameter("id", singletonList(1))
                .parameter("name", singletonList(2))
                .parameter("email", singletonList(3))
                .parameter("ts", singletonList(4))
                .parameter("field1", singletonList(5))
                .parameter("field_2", singletonList(6))
                .parameter("__field_3__", singletonList(7))
                .matches(insertStmt);
    }

    @Test
    public void testDeleteStatement() {
        String deleteStmt = dialect.getDeleteStatement(tableName, keyFields);
        assertEquals("DELETE FROM tbl WHERE id = :id AND __field_3__ = :__field_3__", deleteStmt);
        NamedStatementMatcher.parsedSql("DELETE FROM tbl WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(deleteStmt);
    }

    @Test
    public void testRowExistsStatement() {
        String rowExistStmt = dialect.getRowExistsStatement(tableName, keyFields);
        assertEquals(
                "SELECT 1 FROM tbl WHERE id = :id AND __field_3__ = :__field_3__", rowExistStmt);
        NamedStatementMatcher.parsedSql("SELECT 1 FROM tbl WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(rowExistStmt);
    }

    @Test
    public void testUpdateStatement() {
        String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, keyFields);
        assertEquals(
                "UPDATE tbl SET id = :id, name = :name, email = :email, ts = :ts, "
                        + "field1 = :field1, field_2 = :field_2, __field_3__ = :__field_3__ "
                        + "WHERE id = :id AND __field_3__ = :__field_3__",
                updateStmt);
        NamedStatementMatcher.parsedSql(
                        "UPDATE tbl SET id = ?, name = ?, email = ?, ts = ?, field1 = ?, "
                                + "field_2 = ?, __field_3__ = ? WHERE id = ? AND __field_3__ = ?")
                .parameter("id", asList(1, 8))
                .parameter("name", singletonList(2))
                .parameter("email", singletonList(3))
                .parameter("ts", singletonList(4))
                .parameter("field1", singletonList(5))
                .parameter("field_2", singletonList(6))
                .parameter("__field_3__", asList(7, 9))
                .matches(updateStmt);
    }

    @Test
    public void testUpsertStatement() {
        String upsertStmt = dialect.getUpsertStatement(tableName, fieldNames, keyFields).get();
        assertEquals(
                " MERGE INTO tbl t "
                        + " USING (SELECT :id id, :name name, :email email, :ts ts, :field1 field1, :field_2 field_2, :__field_3__ __field_3__ FROM DUAL) s "
                        + " ON (t.id=s.id and t.__field_3__=s.__field_3__) "
                        + " WHEN MATCHED THEN UPDATE SET t.name=s.name, t.email=s.email, t.ts=s.ts, t.field1=s.field1, t.field_2=s.field_2"
                        + " WHEN NOT MATCHED THEN INSERT (id, name, email, ts, field1, field_2, __field_3__)"
                        + " VALUES (s.id, s.name, s.email, s.ts, s.field1, s.field_2, s.__field_3__)",
                upsertStmt);
        NamedStatementMatcher.parsedSql(
                        " MERGE INTO tbl t "
                                + " USING (SELECT ? id, ? name, ? email, ? ts, ? field1, ? field_2, ? __field_3__ FROM DUAL) s "
                                + " ON (t.id=s.id and t.__field_3__=s.__field_3__) "
                                + " WHEN MATCHED THEN UPDATE SET t.name=s.name, t.email=s.email, t.ts=s.ts, t.field1=s.field1, t.field_2=s.field_2"
                                + " WHEN NOT MATCHED THEN INSERT (id, name, email, ts, field1, field_2, __field_3__)"
                                + " VALUES (s.id, s.name, s.email, s.ts, s.field1, s.field_2, s.__field_3__)")
                .parameter("id", singletonList(1))
                .parameter("name", singletonList(2))
                .parameter("email", singletonList(3))
                .parameter("ts", singletonList(4))
                .parameter("field1", singletonList(5))
                .parameter("field_2", singletonList(6))
                .parameter("__field_3__", singletonList(7))
                .matches(upsertStmt);
    }

    @Test
    public void testSelectStatement() {
        String selectStmt = dialect.getSelectFromStatement(tableName, fieldNames, keyFields);
        assertEquals(
                "SELECT id, name, email, ts, field1, field_2, __field_3__ FROM tbl "
                        + "WHERE id = :id AND __field_3__ = :__field_3__",
                selectStmt);
        NamedStatementMatcher.parsedSql(
                        "SELECT id, name, email, ts, field1, field_2, __field_3__ FROM tbl "
                                + "WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(selectStmt);
    }

    private static class NamedStatementMatcher {
        private String parsedSql;
        private Map<String, List<Integer>> parameterMap = new HashMap<>();

        public static NamedStatementMatcher parsedSql(String parsedSql) {
            NamedStatementMatcher spec = new NamedStatementMatcher();
            spec.parsedSql = parsedSql;
            return spec;
        }

        public NamedStatementMatcher parameter(String name, List<Integer> index) {
            this.parameterMap.put(name, index);
            return this;
        }

        public void matches(String statement) {
            Map<String, List<Integer>> actualParams = new HashMap<>();
            String actualParsedStmt =
                    FieldNamedPreparedStatementImpl.parseNamedStatement(statement, actualParams);
            assertEquals(parsedSql, actualParsedStmt);
            assertEquals(parameterMap, actualParams);
        }
    }
}
