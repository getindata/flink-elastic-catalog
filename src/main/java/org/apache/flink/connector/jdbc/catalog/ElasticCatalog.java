package org.apache.flink.connector.jdbc.catalog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.connector.jdbc.catalog.dialect.ElasticTypeMapper;
import org.apache.flink.connector.jdbc2.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.elasticsearch.xpack.sql.jdbc.EsDriver;

import static org.apache.flink.connector.jdbc.catalog.factory.ElasticJdbcCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.connector.jdbc.catalog.factory.ElasticJdbcCatalogFactoryOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.catalog.factory.ElasticJdbcCatalogFactoryOptions.USERNAME;
import static java.lang.String.format;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc2.table.JdbcConnectorOptions.URL;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;


public class ElasticCatalog extends AbstractJdbcCatalog {

    static {
        try {
            EsDriver.register();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ElasticTypeMapper dialectTypeMapper;

    private final String catalogDefaultScanPartitionColumnName;
    private final String catalogDefaultScanPartitionCapacity;
    private final Map<String, ScanPartitionProperties> scanPartitionProperties;
    private final List<String> indexPatterns;

    public ElasticCatalog(String catalogName,
                          String defaultDatabase,
                          String username,
                          String password,
                          String baseUrl) {
        this(catalogName, defaultDatabase, username, password, baseUrl, new HashMap<>());
    }

    public ElasticCatalog(String catalogName,
                          String defaultDatabase,
                          String username,
                          String password,
                          String baseUrl,
                          Map<String, String> properties) {
        // In elastic the default database name is not a part of defaultUrl, therefore, pass baseUrl
        // as defaultUrl.
        super(catalogName, defaultDatabase, username, password, baseUrl, baseUrl);
        this.dialectTypeMapper = new ElasticTypeMapper();
        String[] catalogDefaultScanProperties = extractCatalogDefaultScanProperties(properties);
        this.indexPatterns = extractIndexPatterns(properties);
        this.catalogDefaultScanPartitionColumnName = catalogDefaultScanProperties[0];
        this.catalogDefaultScanPartitionCapacity = catalogDefaultScanProperties[1];
        this.scanPartitionProperties = extractScanTablePartitionProperties(properties);
    }

    private String[] extractCatalogDefaultScanProperties(Map<String, String> properties) {
        String[] catalogDefaultScanProperties = new String[2];

        if (properties.containsKey("catalog.default.scan.partition.column.name")) {
            catalogDefaultScanProperties[0] = properties.get("catalog.default.scan.partition.column.name");
        }
        if (properties.containsKey("catalog.default.scan.partition.size")) {
            catalogDefaultScanProperties[1] = properties.get("catalog.default.scan.partition.size");
        }

        return catalogDefaultScanProperties;
    }

    private List<String> extractIndexPatterns(Map<String, String> properties) {
        // Splitting patterns and removing duplicates

        return Arrays.stream(
                        properties.getOrDefault("properties.index.patterns", "").split(",")
                )
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    private Map<String, ScanPartitionProperties> extractScanTablePartitionProperties(Map<String, String> properties) {
        Map<String, ScanPartitionProperties> scanPartitionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("properties.scan") || !(key.endsWith("partition.column.name") || key.endsWith("partition.number"))) {
                continue;
            }
            String tableName = key.replace("properties.scan.", "")
                    .replace(".partition.column.name", "")
                    .replace(".partition.number", "");
            boolean scanPropertiesForTableFound = scanPartitionProperties.containsKey(tableName);
            ScanPartitionProperties partitionProperties = scanPropertiesForTableFound
                    ? scanPartitionProperties.get(tableName)
                    : new ScanPartitionProperties();

            if (entry.getKey().endsWith("partition.column.name")) {
                if (partitionProperties.partitionColumnName == null) {
                    partitionProperties.setPartitionColumnName(entry.getValue());
                }
            } else if (entry.getKey().endsWith("partition.number")) {
                if (partitionProperties.partitionNumber == null) {
                    partitionProperties.setPartitionNumber(Integer.parseInt(entry.getValue()));
                }
            }

            // Adding a new scanPartitionProperties to the map
            if (!scanPropertiesForTableFound) {
                scanPartitionProperties.put(tableName, partitionProperties);
            }
        }
        return scanPartitionProperties;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(baseUrl, username, pwd)) {
            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery("SHOW CATALOGS")) {
                while (results.next()) {
                    databases.add(results.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> tables = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(baseUrl, username, pwd)) {
            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery("SHOW TABLES CATALOG '" + databaseName + "'")) {
                while (results.next()) {
                    tables.add(results.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        tables.addAll(indexPatterns);
        return tables;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        try (Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(metaData, null, getSchemaName(tablePath), getTableName(tablePath));

            ResultSetMetaData resultSetMetaData = retrieveResultSetMetaData(conn, tablePath);

            Map<String, DataType> columns = retrieveColumns(resultSetMetaData, tablePath);
            String[] columnNames = columns.keySet().toArray(new String[0]);
            DataType[] types = columns.values().toArray(new DataType[0]);

            Schema tableSchema = buildSchema(columnNames, types, primaryKey);
            String tableName = getSchemaTableName(tablePath);
            ScanPartitionProperties properties = scanPartitionProperties.get(tableName);

            if (shouldTableBePartitioned(properties)) {
                if (properties == null) {
                    properties = new ScanPartitionProperties();
                }

                deducePartitionColumnName(properties, tableName);
                deducePartitionNumber(properties, conn, tableName);

                checkScanPartitionNumber(properties.getPartitionNumber());
                DataType type = retrievePartitionColumnDataType(columnNames, types, properties.getPartitionColumnName(), tableName);

                checkScanPartitionColumnType(type);
                calculateScanPartitionBounds(conn, tableName, isColumnTemporal(type), properties);
            }

            return CatalogTable.of(tableSchema, null, Lists.newArrayList(), createPropertiesMap(tableName, properties));
        } catch (Exception e) {
            throw new CatalogException(format("Failed getting table %s.", tablePath.getFullName()), e);
        }
    }

    private boolean shouldTableBePartitioned(ScanPartitionProperties properties) {
        return properties != null ||
                this.catalogDefaultScanPartitionColumnName != null ||
                this.catalogDefaultScanPartitionCapacity != null;
    }

    private void deducePartitionColumnName(ScanPartitionProperties properties, String tableName) {
        if (properties.partitionColumnName == null) {
            if (this.catalogDefaultScanPartitionColumnName != null) {
                properties.setPartitionColumnName(this.catalogDefaultScanPartitionColumnName);
            } else {
                throw new IllegalArgumentException("Missing column.name property for table " + tableName + " and no catalog default column name specified.");
            }
        }
    }

    private void deducePartitionNumber(ScanPartitionProperties properties, Connection conn, String tableName) {
        if (properties.partitionNumber == null) {
            if (this.catalogDefaultScanPartitionCapacity != null) {
                properties.setPartitionNumber(calculatePartitionNumberBasedOnPartitionSize(conn, tableName));
            } else {
                throw new IllegalArgumentException("Missing partition.number property for table " + tableName + " and no catalog default partition size specified.");
            }
        }
    }

    private void checkScanPartitionNumber(int partitionNumber) {
        if (partitionNumber <= 0) {
            throw new CatalogException("Partition number has to be greater than 0!");
        }
    }

    private DataType retrievePartitionColumnDataType(String[] columnNames, DataType[] types, String partitionColumnName, String tableName) {
        for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
            if (Objects.equals(columnNames[columnIndex], partitionColumnName)) {
                return types[columnIndex];
            }
        }
        throw new CatalogException(format("Partition column was not found in the specified table %s!", tableName));
    }

    private void checkScanPartitionColumnType(DataType type) {
        if (!(isColumnNumeric(type) || isColumnTemporal(type))) {
            throw new CatalogException(
                    format("Partition column is of type %s. We support only NUMERIC, DATE and TIMESTAMP partition columns.", type));
        }
    }

    private Map<String, DataType> retrieveColumns(ResultSetMetaData resultSetMetaData,
                                                  ObjectPath tablePath) throws SQLException {
        Map<String, DataType> columns = new HashMap<>();

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            DataType type = fromJDBCType(tablePath, resultSetMetaData, i);
            if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                type = type.notNull();
            }
            columns.put(columnName, type);
        }
        return columns;
    }

    private ResultSetMetaData retrieveResultSetMetaData(Connection conn, ObjectPath tablePath) throws java.sql.SQLException {
        String query = format("SELECT * FROM \"%s\" LIMIT 0", getSchemaTableName(tablePath));
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        return rs.getMetaData();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Schema buildSchema(String[] columnNames, DataType[] types, Optional<UniqueConstraint> primaryKey) {
        Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
        primaryKey.ifPresent(
                pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
        return schemaBuilder.build();
    }

    private boolean isColumnNumeric(DataType type) {
        return type.equals(DataTypes.TINYINT()) ||
                type.equals(DataTypes.SMALLINT()) ||
                type.equals(DataTypes.INT()) ||
                type.equals(DataTypes.BIGINT()) ||
                type.equals(DataTypes.FLOAT()) ||
                type.equals(DataTypes.DOUBLE());
    }

    private boolean isColumnTemporal(DataType type) {
        return type.equals(DataTypes.TIMESTAMP()) ||
                type.equals(DataTypes.DATE());
    }

    private int calculatePartitionNumberBasedOnPartitionSize(Connection conn, String tableName) {
        try {
            String scanPartitionInfoQuery = format("SELECT CEIL(COUNT(*) / %s) FROM \"%s\"", this.catalogDefaultScanPartitionCapacity, tableName);
            PreparedStatement preparedStatement = conn.prepareStatement(scanPartitionInfoQuery);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();

            return resultSet.getInt(1) > 0 ? resultSet.getInt(1) : 1;
        } catch (SQLException e) {
            throw new CatalogException("There was a problem calculating partition number based on catalog default partition capacity!", e);
        }
    }

    private void calculateScanPartitionBounds(Connection conn, String tableName, boolean isPartitionColumnTemporal, ScanPartitionProperties scanProperties) {
        try {
            String scanPartitionInfoQuery;
            if (isPartitionColumnTemporal) {
                scanPartitionInfoQuery = format("SELECT COALESCE(MIN(%s), CURRENT_DATE) AS scanPartitionLowerBound, DATE_ADD('days', 1, CURRENT_DATE) AS scanPartitionUpperBound FROM \"%s\"", scanProperties.getPartitionColumnName(), tableName);
            } else {
                scanPartitionInfoQuery = format("SELECT MIN(%s) AS scanPartitionLowerBound, MAX(%s) AS scanPartitionUpperBound FROM \"%s\"", scanProperties.getPartitionColumnName(), scanProperties.getPartitionColumnName(), tableName);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(scanPartitionInfoQuery);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();

            if (isPartitionColumnTemporal) {
                scanProperties.setScanPartitionLowerBound(resultSet.getTimestamp(1).getTime());
                scanProperties.setScanPartitionUpperBound(resultSet.getTimestamp(2).getTime() - 1);
            } else {
                scanProperties.setScanPartitionLowerBound(resultSet.getLong(1));
                scanProperties.setScanPartitionUpperBound(resultSet.getLong(2));
            }
        } catch (SQLException e) {
            throw new CatalogException("There was a problem with calculating the scan partition bounds!", e);
        }
    }

    private Map<String, String> createPropertiesMap(String tableName, ScanPartitionProperties scanProperties) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), IDENTIFIER);
        properties.put(URL.key(), baseUrl);
        properties.put(USERNAME.key(), username);
        properties.put(PASSWORD.key(), pwd);
        properties.put(TABLE_NAME.key(), tableName);
        if (scanProperties != null) {
            properties.put(SCAN_PARTITION_COLUMN.key(), scanProperties.getPartitionColumnName());
            properties.put(SCAN_PARTITION_NUM.key(), String.valueOf(scanProperties.getPartitionNumber()));
            properties.put(SCAN_PARTITION_LOWER_BOUND.key(), String.valueOf(scanProperties.getLowerBound()));
            properties.put(SCAN_PARTITION_UPPER_BOUND.key(), String.valueOf(scanProperties.getUpperBound()));
        }
        return properties;
    }

    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    public String getCatalogDefaultScanPartitionColumnName() {
        return catalogDefaultScanPartitionColumnName;
    }

    public String getCatalogDefaultScanPartitionCapacity() {
        return catalogDefaultScanPartitionCapacity;
    }

    public Map<String, ScanPartitionProperties> getScanPartitionProperties() {
        return scanPartitionProperties;
    }

    public List<String> getIndexPatterns() {
        return indexPatterns;
    }

    public static class ScanPartitionProperties {
        private String partitionColumnName;
        private Integer partitionNumber;
        private Long scanPartitionLowerBound;
        private Long scanPartitionUpperBound;

        public ScanPartitionProperties() {
            this.partitionColumnName = null;
            this.partitionNumber = null;
            this.scanPartitionLowerBound = 0L;
            this.scanPartitionUpperBound = 0L;
        }

        public String getPartitionColumnName() {
            return this.partitionColumnName;
        }

        public Integer getPartitionNumber() {
            return this.partitionNumber;
        }

        public Long getLowerBound() {
            return this.scanPartitionLowerBound;
        }

        public Long getUpperBound() {
            return this.scanPartitionUpperBound;
        }

        public void setPartitionColumnName(String columnName) {
            this.partitionColumnName = columnName;
        }

        public void setPartitionNumber(int columnNumber) {
            this.partitionNumber = columnNumber;
        }

        public void setScanPartitionLowerBound(Long lowerBound) {
            this.scanPartitionLowerBound = lowerBound;
        }

        public void setScanPartitionUpperBound(Long upperBound) {
            this.scanPartitionUpperBound = upperBound;
        }

        public String toString() {
            return "partitionColumnName=" + partitionColumnName +
                    ", partitionNumber=" + partitionNumber +
                    ", scanPartitionLowerBound=" + scanPartitionLowerBound +
                    ", scanPartitionUpperBound=" + scanPartitionUpperBound;
        }
    }
}
