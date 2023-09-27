package com.getindata.flink.connector.jdbc.catalog;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.getindata.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

public class ElasticTypeMapper implements JdbcDialectTypeMapper {

    private static final String ELASTIC_TEXT = "TEXT";
    private static final String ELASTIC_BOOLEAN = "BOOLEAN";
    private static final String ELASTIC_BYTE = "BYTE";
    private static final String ELASTIC_KEYWORD = "KEYWORD";
    private static final String ELASTIC_DATETIME = "DATETIME";
    private static final String ELASTIC_DOUBLE = "DOUBLE";
    private static final String ELASTIC_FLOAT = "FLOAT";
    private static final String ELASTIC_HALF_FLOAT = "HALF_FLOAT";
    private static final String ELASTIC_INTEGER = "INTEGER";
    private static final String ELASTIC_IP = "IP";
    private static final String ELASTIC_LONG = "LONG";
    private static final String ELASTIC_SCALED_FLOAT = "SCALED_FLOAT";
    private static final String ELASTIC_SHORT = "SHORT";

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {

        String elasticType = metadata.getColumnTypeName(colIndex);

        switch (elasticType) {
            case ELASTIC_TEXT:
            case ELASTIC_KEYWORD:
            case ELASTIC_IP:
                return DataTypes.STRING();
            case ELASTIC_BOOLEAN:
                return DataTypes.BOOLEAN();
            case ELASTIC_BYTE:
                return DataTypes.TINYINT();
            case ELASTIC_DATETIME:
                int p = metadata.getPrecision(colIndex);
                if (p > 0) {
                    return DataTypes.TIMESTAMP(p);
                }
                else {
                    return DataTypes.TIMESTAMP();
                }
            case ELASTIC_DOUBLE:
            case ELASTIC_SCALED_FLOAT:
                return DataTypes.DOUBLE();
            case ELASTIC_FLOAT:
            case ELASTIC_HALF_FLOAT:
                return DataTypes.FLOAT();
            case ELASTIC_INTEGER:
                return DataTypes.INT();
            case ELASTIC_LONG:
                return DataTypes.BIGINT();
            case ELASTIC_SHORT:
                return DataTypes.SMALLINT();
        }
        throw new UnsupportedOperationException("We do not support the data type '" + elasticType + "' for column '" + metadata.getColumnName(colIndex) + "'!");
    }
}
