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

package com.getindata.flink.connector.jdbc.table;

import com.getindata.flink.connector.jdbc.split.CompositeJdbcParameterValuesProvider;
import com.getindata.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import com.getindata.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import com.getindata.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.annotation.Internal;
import com.getindata.flink.connector.jdbc.dialect.JdbcDialect;
import com.getindata.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import com.getindata.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import com.getindata.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** A {@link DynamicTableSource} for JDBC. */
@Internal
public class JdbcDynamicTableSource
    implements ScanTableSource,
    LookupTableSource,
    SupportsProjectionPushDown,
    SupportsLimitPushDown,
    SupportsFilterPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcDynamicTableSource.class);

    private final JdbcConnectorOptions options;
    private final JdbcReadOptions readOptions;

    private final JdbcLookupOptions lookupOptions;
    private DataType physicalRowDataType;
    private final String dialectName;
    private long limit = -1;
    private List<String> resolvedPredicates = new ArrayList<>();
    private Serializable[] pushdownParams = new Serializable[0];

    public JdbcDynamicTableSource(
        JdbcConnectorOptions options,
        JdbcReadOptions readOptions,
        JdbcLookupOptions lookupOptions,
        DataType physicalRowDataType) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = options.getDialect().dialectName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        return TableFunctionProvider.of(
                new JdbcRowDataLookupFunction(
                        options,
                        lookupOptions,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        rowType));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder =
            JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        String query =
            dialect.getSelectFromStatement(
                options.getTableName(),
                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                new String[0]);
        final List<String> predicates = new ArrayList<String>();

        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();

            Serializable[][] allPushdownParams = replicatePushdownParamsForN(numPartitions);
            JdbcParameterValuesProvider allParams =
                new CompositeJdbcParameterValuesProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                        .ofBatchNum(numPartitions),
                    new JdbcGenericParameterValuesProvider(allPushdownParams));

            builder.setParametersProvider(allParams);

            predicates.add(
                dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                    + " BETWEEN ? AND ?");
        } else {
            builder.setParametersProvider(
                new JdbcGenericParameterValuesProvider(replicatePushdownParamsForN(1)));
        }

        predicates.addAll(this.resolvedPredicates);

        if (predicates.size() > 0) {
            String joinedConditions =
                predicates.stream()
                    .map(pred -> String.format("(%s)", pred))
                    .collect(Collectors.joining(" AND "));
            query += " WHERE " + joinedConditions;
        }

        if (limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }

        LOG.debug("Query generated for JDBC scan: " + query);

        builder.setQuery(query);
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(
            runtimeProviderContext.createTypeInformation(physicalRowDataType));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public DynamicTableSource copy() {
        JdbcDynamicTableSource newSource =
            new JdbcDynamicTableSource(
                options, readOptions, lookupOptions, physicalRowDataType);
        newSource.resolvedPredicates = new ArrayList<>(this.resolvedPredicates);
        newSource.pushdownParams = Arrays.copyOf(this.pushdownParams, this.pushdownParams.length);
        return newSource;
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
            && Objects.equals(readOptions, that.readOptions)
            && Objects.equals(physicalRowDataType, that.physicalRowDataType)
            && Objects.equals(dialectName, that.dialectName)
            && Objects.equals(limit, that.limit)
            && Objects.equals(resolvedPredicates, that.resolvedPredicates)
            && Arrays.deepEquals(pushdownParams, that.pushdownParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            options,
            readOptions,
            physicalRowDataType,
            dialectName,
            limit,
            resolvedPredicates,
            pushdownParams);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            Optional<ParameterizedPredicate> simplePredicate = parseFilterToPredicate(filter);
            if (simplePredicate.isPresent()) {
                acceptedFilters.add(filter);
                ParameterizedPredicate pred = simplePredicate.get();
                this.pushdownParams = ArrayUtils.addAll(this.pushdownParams, pred.getParameters());
                this.resolvedPredicates.add(pred.getPredicate());
            } else {
                remainingFilters.add(filter);
            }
        }

        return Result.of(acceptedFilters, remainingFilters);
    }

    private Optional<ParameterizedPredicate> parseFilterToPredicate(ResolvedExpression filter) {
        if (filter instanceof CallExpression) {
            CallExpression callExp = (CallExpression) filter;
            return callExp.accept(
                new JdbcFilterPushdownPreparedStatementVisitor(
                    this.options.getDialect()::quoteIdentifier));
        }
        return Optional.empty();
    }

    private Serializable[][] replicatePushdownParamsForN(int n) {
        Serializable[][] allPushdownParams = new Serializable[n][pushdownParams.length];
        for (int i = 0; i < n; i++) {
            allPushdownParams[i] = this.pushdownParams;
        }
        return allPushdownParams;
    }
}
