# flink-elastic-catalog

---

## Description

This is an implementation of a [Flink Catalog](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/catalogs/)
for [Elastic](https://www.elastic.co/).
Using this catalog (For example in EMR Notebooks) users will be able to get metadata information about the objects in
Elastic.

---

## Possible Operations

- `listDatabases` Lists Databases in a catalog.
- `databaseExists` Checks if a database exists.
- `listTables` Lists Tables in a Database.
- `tableExists` Checks if a table exists.
- `getTable` Gets the metadata information about the table. This consists of table schema and table properties. Table properties among others contain `CONNECTOR`, `BASE_URL`, `TABLE_NAME` and `SCAN_PARTITION` options.

---

## Scan options

If we want tables in a catalog to be partitioned by a column we should specify scan options.
It is possible to set up [Scan options](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/jdbc/#scan-partition-column:~:text=than%201%20second.-,scan.partition.column,-optional) while defining a catalog.

There are 2 types of scan options for Elastic Catalog:

### Default scan options for a catalog
We can specify default partitioning options for all tables in a catalog. If no options for a table are specified, these options will be used to
select a column for partitioning and the number of partitions for a table will be calculated based on catalog default option. 

- `catalog.default.scan.partition.column.name` Specify what column to use for table partitioning by default. The default option will be used
for all tables in a catalog. We can overwrite a column to use for partitioning of a table by specifying table specific scan options.
- `catalog.default.scan.partition.size` Specify how many elements should be placed in a single partition. The number of
partitions will be calculated based on the number of elements and the default size of a partition. If we want a particular table
to have an exact number of partitions, we can specify that number using table specific scan options.

### Table specific scan options
These options can be useful if we know that not all tables in a catalog should be partitioned in the same way. Here
we can specify partitioning options for selected tables.

- `properties.scan.{tablename}.partition.column.name` Specify the name of the column to use for partitioning of a table.
Corresponds to the `scan.partition.column` option.
- `properties.scan.{tablename}.partition.number` Specify the number of partitions for a table. Corresponds to the `scan.partition.num` option.

For both of options specified above we should replace `{tablename}` with the name of the table that we want the options to apply to.
We can provide these options for multiple tables.

### Index patterns
If we specify an index pattern, a Flink table will be created in Catalog that instead of targeting a single index in Elastic will target all indexes that match
the pattern provided. It is useful to use if we want to write Flink SQL that reads similar data from many similar tables instead of a single one.
The resulting Flink table will contain all columns found in matching tables and will use all the data from matching tables.
This table will have the same name as the pattern.

- `properties.index.patterns` Specify patterns for which we want to create Flink tables. We can specify multiple index patterns by
separating them with a comma `,` sign.

The Flink tables created this way can also be partitioned just as other Flink tables by providing default catalog scan options or table specific scan options.

---

## Rules for overwriting catalog scan options

### No scan options were provided
There is no necessity to provide either default scan options for a catalog or table specific scan options. If there are no scan options provided
no tables in a catalog will be partitioned.

### Only default scan options for a catalog were provided
If only default catalog scan options were provided, all tables in a catalog will be partitioned in a similar way. The same column name for table partitioning for all tables and
the number of partitions for tables will be dependant on the number of records in a table. All tables will have the same maximum number of elements in a partition.

### Only table specific scan options were provided
If we want a specific table to be partitioned and leave the rest of tables nonpartitioned we have to provide both table specific scan options.

### We specified both catalog default scan options and table specific scan were options
Table specific scan options have higher priority over catalog default scan properties when deciding how to partition a table.
If we specify catalog default partition column name and a table specific partition column name then table specific partition column name is taken into account.
Similar thing happens when we specify catalog default scan partition size and table specific partition number. Instead of calculating the number of partitions for a table
based on the count of elements, the table will have the number of partitions equal to the one provided for a table.

--- 

## Calculation of scan partition bounds
If a table is partitioned, meaning that we specified catalog default scan options or we specified table specific scan options the upper and lower bounds will be calculated.
As specified in the Flink documentation, the `properties.scan.{tablename}.partition.column.name` option works for numeric and temporal data types.
The `scan.partition.lower-bound` will be calculated as the lowest value in the table.
The `scan.partition.upper-bound` will be calculated as the highest value in the table.

---

## Note that
If we want a table to be partitioned it is necessary that we provide a catalog default or table specific option for partition column to use and
catalog default or table specific partition number option for deciding how many partitions to use for a table.
If only 1 option is provided we will receive an error.

---

# Internals

## flink-connector-jdbc

The project contains clones of `AbstractJdbcCatalog` and `JdbcCatalogUtils`. `AbstractJdbcCatalog` differs only by
constructor in which `baseUrl` and `defaultUrl` are set differently.

```java
this.baseUrl = baseUrl;
this.defaultUrl = this.baseUrl;
```

`JdbcCatalogUtils` differs on jdbc url validation condition.

```java
checkArgument(parts.length == 2 || parts.length == 3);
```

