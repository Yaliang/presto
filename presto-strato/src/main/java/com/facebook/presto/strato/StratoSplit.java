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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public class StratoSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Optional<String> key;
    private final URI uri;
    private final Map<String, String> queryMap;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;
    private final Map<Integer, String> prefilledValues;

    @JsonCreator
    public StratoSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("key") Optional<String> key,
            @JsonProperty("uri") URI uri,
            @JsonProperty("queryMap") Map<String, String> queryMap)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.key = requireNonNull(key, "key is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.queryMap = queryMap;

        remotelyAccessible = true;
        addresses = ImmutableList.of(HostAddress.fromUri(uri));

        prefilledValues = ImmutableMap.<Integer, String>builder()
                .put(2, nullToEmpty(queryMap.get("pkey")))
                .put(3, nullToEmpty(queryMap.get("from")))
                .put(4, nullToEmpty(queryMap.get("to")))
                .put(5, nullToEmpty(queryMap.get("view")))
                .put(6, nullToEmpty(queryMap.get("prefix")))
                .put(7, nullToEmpty(queryMap.get("limit")))
                .build();
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public Map<String, String> getQueryMap()
    {
        return queryMap;
    }

    @JsonProperty
    public Optional<String> getKey()
    {
        return key;
    }

    public Map<Integer, String> getPrefilledValues()
    {
        return prefilledValues;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
