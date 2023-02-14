package com.getindata.flink.connector.jdbc.internal.converter;

import com.getindata.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class ElasticRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Elasticsearch";
    }

    public ElasticRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return val -> val;
            case DOUBLE:
                return val -> val;
            case FLOAT:
                return val -> val;
            default:
                return super.createInternalConverter(type);
        }
    }

    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
            default:
                return super.createExternalConverter(type);
        }
    }

}
