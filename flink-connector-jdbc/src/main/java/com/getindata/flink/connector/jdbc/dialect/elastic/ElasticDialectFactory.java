package com.getindata.flink.connector.jdbc.dialect.elastic;

import com.getindata.flink.connector.jdbc.dialect.JdbcDialect;
import com.getindata.flink.connector.jdbc.dialect.JdbcDialectFactory;
import org.apache.flink.annotation.Internal;

/**
 * Factory for {@link ElasticDialect}.
 */
@Internal
public class ElasticDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:elasticsearch:") || url.startsWith("jdbc:es:");
    }

    @Override
    public JdbcDialect create() {
        return new ElasticDialect();
    }
}
