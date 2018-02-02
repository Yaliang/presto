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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StratoTable
{
    private final String space;
    private final String name;
    private final List<StratoColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public StratoTable(
            @JsonProperty("space") String space,
            @JsonProperty("name") String name)
    {
        checkArgument(!isNullOrEmpty(space), "space is null or is empty");
        this.space = requireNonNull(space, "space is null");
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.of(
                new StratoColumn("key", createUnboundedVarcharType(), false),
                new StratoColumn("value", createUnboundedVarcharType(), false),
                new StratoColumn("_from", createUnboundedVarcharType(), true),
                new StratoColumn("_to", createUnboundedVarcharType(), true),
                new StratoColumn("_view", createUnboundedVarcharType(), true),
                new StratoColumn("_prefix", createUnboundedVarcharType(), true),
                new StratoColumn("_limit", createUnboundedVarcharType(), true));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (StratoColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(), "", column.isHidden()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getSpace()
    {
        return space;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<StratoColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getURL()
    {
        return format("https://strato.twitter.biz/column/%s//%s", space, name);
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
