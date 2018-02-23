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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;

public class StratoTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final StratoTableHandle table;
    private final List<String> keys;
    private final String url;
    private final Map<String, String> queryMap;
    private final String queryString;

    @JsonCreator
    public StratoTableLayoutHandle(
            @JsonProperty("table") StratoTableHandle table,
            @JsonProperty("keys") List<String> keys,
            @JsonProperty("pkey") String pkey,
            @JsonProperty("from") String from,
            @JsonProperty("to") String to,
            @JsonProperty("view") String view,
            @JsonProperty("prefix") String prefix,
            @JsonProperty("limit") String limit,
            @JsonProperty("url") String url)
    {
        this.table = table;
        this.keys = keys;
        this.url = url;
        this.queryMap = ImmutableMap.<String, String>builder()
                .put("pkey", nullToEmpty(pkey))
                .put("from", nullToEmpty(from))
                .put("to", nullToEmpty(to))
                .put("view", nullToEmpty(view))
                .put("prefix", nullToEmpty(prefix))
                .put("limit", nullToEmpty(limit))
                .build();
        this.queryString = queryMap.entrySet().stream()
                .filter(e -> e.getValue() != null && e.getValue().length() > 0)
                .map(e -> format("%s=%s", e.getKey(), UrlEscapers.urlFormParameterEscaper().escape(e.getValue())))
                .reduce((a, b) -> format("%s&%s", a, b))
                .orElse("");
    }

    @JsonProperty
    public StratoTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<String> getKeys()
    {
        return keys;
    }

    @JsonProperty
    public String getUrl()
    {
        return url;
    }

    public Map<String, String> getQueryMap()
    {
        return queryMap;
    }

    public String getFormattedUrl(String key)
    {
        return format(url, key) + (queryString == null || queryString.length() == 0 ? "" : "?" + queryString);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StratoTableLayoutHandle that = (StratoTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(url, that.url);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
