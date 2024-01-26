package com.getindata.flink.connector.jdbc.catalog;

import java.util.Map;

public final class ElasticCatalogBuilder {
    private ClassLoader userClassLoader;
    private String username;
    private String password;
    private String baseUrl;
    private boolean addProctimeColumn;
    private IndexFilterResolver indexFilterResolver;
    private String catalogName;
    private String defaultDatabase;

    private Map<String, String> properties;

    private ElasticCatalogBuilder() {
    }

    public static ElasticCatalogBuilder anElasticCatalog() {
        return new ElasticCatalogBuilder();
    }

    public ElasticCatalogBuilder userClassLoader(ClassLoader userClassLoader) {
        this.userClassLoader = userClassLoader;
        return this;
    }

    public ElasticCatalogBuilder username(String username) {
        this.username = username;
        return this;
    }

    public ElasticCatalogBuilder password(String password) {
        this.password = password;
        return this;
    }

    public ElasticCatalogBuilder baseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public ElasticCatalogBuilder addProctimeColumn(boolean addProctimeColumn) {
        this.addProctimeColumn = addProctimeColumn;
        return this;
    }

    public ElasticCatalogBuilder indexFilterResolver(IndexFilterResolver indexFilterResolver) {
        this.indexFilterResolver = indexFilterResolver;
        return this;
    }

    public ElasticCatalogBuilder catalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public ElasticCatalogBuilder defaultDatabase(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
        return this;
    }


    public ElasticCatalogBuilder properties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public ElasticCatalog build() {
        return new ElasticCatalog(userClassLoader, catalogName, defaultDatabase, username, password, baseUrl,
                addProctimeColumn, indexFilterResolver, properties);
    }
}
