package com.getindata.flink.connector.jdbc.dialect.elastic;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import com.getindata.flink.connector.jdbc.converter.JdbcRowConverter;
import com.getindata.flink.connector.jdbc.dialect.AbstractDialect;
import com.getindata.flink.connector.jdbc.internal.converter.ElasticRowConverter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * JDBC dialect for Elastic.
 */
@Internal
public class ElasticDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "Elasticsearch";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.elasticsearch.xpack.sql.jdbc.EsDriver");
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // TODO: verify
        return EnumSet.of(
            LogicalTypeRoot.BOOLEAN,
            LogicalTypeRoot.TINYINT,
            LogicalTypeRoot.SMALLINT,
            LogicalTypeRoot.INTEGER,
            LogicalTypeRoot.BIGINT,
            LogicalTypeRoot.FLOAT,
            LogicalTypeRoot.DOUBLE,
            LogicalTypeRoot.VARBINARY,
            LogicalTypeRoot.VARCHAR,
            LogicalTypeRoot.STRUCTURED_TYPE,
            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
        );
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(1, 10));
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new ElasticRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return '"' + identifier + '"';
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName,
                                               String[] fieldNames,
                                               String[] uniqueKeyFields) {
        throw new UnsupportedOperationException("Upsert is not implemented.");
    }

    @Override
    public String getInsertIntoStatement(String tableName,
                                                   String[] fieldNames) {
        throw new UnsupportedOperationException("Insert into is not implemented.");
    }

    @Override
    public String getUpdateStatement(String tableName,
                                     String[] fieldNames,
                                     String[] conditionFields) {
        throw new UnsupportedOperationException("Update is not implemented.");
    }

    @Override
    public String getDeleteStatement(String tableName,
                                     String[] conditionFields) {
        throw new UnsupportedOperationException("Delete is not implemented.");
    }

}
