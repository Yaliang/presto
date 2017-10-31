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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.pulsar.client.api.MessageId;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PulsarSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String serviceUrl;
    private final String topicName;
    private final String keyDataFormat;
    private final String messageDataFormat;
    private final int partitionId;
    private final MessageId start;
    private final MessageId end;

    @JsonCreator
    public PulsarSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("serviceUrl") String serviceUrl,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("start") MessageId start,
            @JsonProperty("end") MessageId end)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.serviceUrl = requireNonNull(serviceUrl, "service url is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "dataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.partitionId = partitionId;
        this.start = start;
        this.end = end;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getServiceUrl()
    {
        return serviceUrl;
    }

    @JsonProperty
    public MessageId getStart()
    {
        return start;
    }

    @JsonProperty
    public MessageId getEnd()
    {
        return end;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("serviceUrl", serviceUrl)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("partitionId", partitionId)
                .add("start", start)
                .add("end", end)
                .toString();
    }
}
