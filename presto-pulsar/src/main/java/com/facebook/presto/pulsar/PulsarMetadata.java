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
package com.facebook.presto.pulsar;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.pulsar.PulsarHandleResolver.convertColumnHandle;
import static com.facebook.presto.pulsar.PulsarHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Pulsar connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link PulsarInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class PulsarMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final boolean hideInternalColumns;
    private final Map<SchemaTableName, PulsarTopicDescription> tableDescriptions;
    private final Set<PulsarInternalFieldDescription> internalFieldDescriptions;

    @Inject
    public PulsarMetadata(
            PulsarConnectorId connectorId,
            PulsarConnectorConfig pulsarConnectorConfig,
            Supplier<Map<SchemaTableName, PulsarTopicDescription>> pulsarTableDescriptionSupplier,
            Set<PulsarInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(pulsarConnectorConfig, "pulsarConfig is null");
        this.hideInternalColumns = pulsarConnectorConfig.isHideInternalColumns();

        requireNonNull(pulsarTableDescriptionSupplier, "pulsarTableDescriptionSupplier is null");
        this.tableDescriptions = pulsarTableDescriptionSupplier.get();
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public PulsarTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        PulsarTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new PulsarTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()));
    }

    private static String getDataFormat(PulsarTopicFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PulsarTableHandle pulsarTableHandle = convertTableHandle(tableHandle);

        PulsarTopicDescription pulsarTopicDescription = tableDescriptions.get(pulsarTableHandle.toSchemaTableName());
        if (pulsarTopicDescription == null) {
            throw new TableNotFoundException(pulsarTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        PulsarTopicFieldGroup key = pulsarTopicDescription.getKey();
        if (key != null) {
            List<PulsarTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (PulsarTopicFieldDescription pulsarTopicFieldDescription : fields) {
                    columnHandles.put(pulsarTopicFieldDescription.getName(), pulsarTopicFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }

        PulsarTopicFieldGroup message = pulsarTopicDescription.getMessage();
        if (message != null) {
            List<PulsarTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (PulsarTopicFieldDescription pulsarTopicFieldDescription : fields) {
                    columnHandles.put(pulsarTopicFieldDescription.getName(), pulsarTopicFieldDescription.getColumnHandle(connectorId, false, index++));
                }
            }
        }

        for (PulsarInternalFieldDescription pulsarInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(pulsarInternalFieldDescription.getName(), pulsarInternalFieldDescription.getColumnHandle(connectorId, index++, hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        PulsarTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new PulsarTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        PulsarTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        PulsarTopicFieldGroup key = table.getKey();
        if (key != null) {
            List<PulsarTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (PulsarTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        PulsarTopicFieldGroup message = table.getMessage();
        if (message != null) {
            List<PulsarTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (PulsarTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (PulsarInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
