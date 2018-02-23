/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.strato;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StratoMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(StratoMetadata.class);

    private final String connectorId;
    private final Map<SchemaTableName, StratoTable> tables;

    @Inject
    public StratoMetadata(StratoConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.tables = ImmutableMap.of(new SchemaTableName("default", "strato_columnpaths"), new StratoTable("None", "strato/columnPaths"),
                new SchemaTableName("tweet", "core"), new StratoTable("Tweet", "core"),
                new SchemaTableName("screen_name", "gizmoduck_core"), new StratoTable("ScreenName", "gizmoduck/core"),
                new SchemaTableName("user", "timeline_home"), new StratoTable("User", "timeline/home"),
                new SchemaTableName("user", "gizmoduck_core"), new StratoTable("User", "gizmoduck/core"));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("default", "tweet", "screen_name", "user");
    }

    @Override
    public StratoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tables.keySet().stream().noneMatch(k -> k.equals(tableName))) {
            return null;
        }

        return new StratoTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        StratoTableHandle tableHandle = (StratoTableHandle) table;
        StratoTable stratoTable = tables.get(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()));
        log.debug(constraint.getSummary().toString(session));
        Map<String, String> queries = getQueries(constraint);
        List<String> keys = getKeys(constraint);
        Optional<Range> spanRange = getKeySpan(constraint);
        log.debug(queries.toString());
        log.debug(keys.toString());

        ConnectorTableLayout layout = new ConnectorTableLayout(new StratoTableLayoutHandle(
                tableHandle,
                keys,
                queries.getOrDefault("_pkey", ""),
                queries.getOrDefault("_from", spanRange.isPresent() ? ((Slice) spanRange.get().getLow().getValue()).toStringUtf8() : ""),
                queries.getOrDefault("_to", spanRange.isPresent() ? ((Slice) spanRange.get().getHigh().getValue()).toStringUtf8() : ""),
                queries.getOrDefault("_view", ""),
                queries.getOrDefault("_prefix", ""),
                queries.getOrDefault("_limit", ""),
                stratoTable.getURL()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    private Map<String, String> getQueries(Constraint<ColumnHandle> constraint)
    {
        Map<String, String> queries = new HashMap<>();
        constraint.getSummary().getDomains().orElse(new HashMap<>()).entrySet().stream()
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getKey() instanceof StratoColumnHandle)
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getValue().isSingleValue())
                .forEach(columnHandleDomainEntry -> queries.put(((StratoColumnHandle) columnHandleDomainEntry.getKey()).getColumnName(), ((Slice) columnHandleDomainEntry.getValue().getSingleValue()).toStringUtf8()));
        return queries;
    }

    private List<String> getKeys(Constraint<ColumnHandle> constraint)
    {
        return constraint.getSummary().getDomains().orElse(new HashMap<>()).entrySet().stream()
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getKey() instanceof StratoColumnHandle)
                .filter(columnHandleDomainEntry -> ((StratoColumnHandle) columnHandleDomainEntry.getKey()).getColumnName().equals("key"))
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getValue().getValues() instanceof SortedRangeSet)
                .map(columnHandleDomainEntry -> columnHandleDomainEntry.getValue().getValues().getRanges().getOrderedRanges().stream()
                        .filter(Range::isSingleValue)
                        .map(range -> ((Slice) range.getSingleValue()).toStringUtf8()))
                .map(values -> ImmutableList.copyOf(values.iterator()))
                .findFirst()
                .orElse(ImmutableList.of());
    }

    private Optional<Range> getKeySpan(Constraint<ColumnHandle> constraint)
    {
        return constraint.getSummary().getDomains().orElse(new HashMap<>()).entrySet().stream()
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getKey() instanceof StratoColumnHandle)
                .filter(columnHandleDomainEntry -> ((StratoColumnHandle) columnHandleDomainEntry.getKey()).getColumnName().equals("key"))
                .filter(columnHandleDomainEntry -> columnHandleDomainEntry.getValue().getValues() instanceof SortedRangeSet)
                .map(columnHandleDomainEntry -> ((SortedRangeSet) columnHandleDomainEntry.getValue().getValues()).getSpan())
                .findFirst();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StratoTableHandle stratoTableHandle = (StratoTableHandle) table;
        checkArgument(stratoTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(stratoTableHandle.getSchemaName(), stratoTableHandle.getTableName());

        return new ConnectorTableMetadata(tableName, tables.get(tableName).getColumnsMetadata());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.copyOf(tables.keySet());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StratoTableHandle stratoTableHandleTableHandle = (StratoTableHandle) tableHandle;
        checkArgument(stratoTableHandleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        StratoTable table = tables.get(new SchemaTableName(stratoTableHandleTableHandle.getSchemaName(), stratoTableHandleTableHandle.getTableName()));
        if (table == null) {
            throw new TableNotFoundException(stratoTableHandleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int i = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new StratoColumnHandle(connectorId, column.getName(), column.getType(), i));
            i = i + 1;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables.keySet()) {
            if (prefix.getSchemaName() != null && !tableName.getSchemaName().startsWith(prefix.getSchemaName())) {
                continue;
            }
            if (prefix.getTableName() != null && !tableName.getTableName().startsWith(prefix.getTableName())) {
                continue;
            }
            columns.put(tableName, tables.get(tableName).getColumnsMetadata());
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StratoColumnHandle) columnHandle).getColumnMetadata();
    }
}
