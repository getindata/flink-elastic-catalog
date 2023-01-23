package org.apache.flink.connector.jdbc2.dialect.elastic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc2.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc2.dialect.JdbcDialectFactory;

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
