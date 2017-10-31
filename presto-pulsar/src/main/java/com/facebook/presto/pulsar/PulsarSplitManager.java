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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.ConsumerImpl;

import javax.inject.Inject;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.pulsar.PulsarErrorCode.PULSAR_ADMIN_ERROR;
import static com.facebook.presto.pulsar.PulsarErrorCode.PULSAR_READER_ERROR;
import static com.facebook.presto.pulsar.PulsarHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

/**
 * Pulsar specific implementation of {@link ConnectorSplitManager}.
 */
public class PulsarSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PulsarSplitManager.class);

    private final String connectorId;
    private final PulsarClientManager clientManager;
    private final PulsarConnectorConfig connectorConfig;

    @Inject
    public PulsarSplitManager(
            PulsarConnectorId connectorId,
            PulsarConnectorConfig connectorConfig,
            PulsarClientManager clientManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientManager = requireNonNull(clientManager, "consumerManager is null");
        this.connectorConfig = requireNonNull(connectorConfig, "pulsarConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        System.out.println("Start get splits");
        try {
            PulsarTableHandle tableHandle = convertLayout(layout).getTable();
            PulsarAdmin admin = clientManager.getAdmin(new URL(connectorConfig.getWebServiceUrl()));
            Reader reader = clientManager.getReader(connectorConfig.getServiceUrl(), tableHandle.getTopicName(), MessageId.earliest);
            String subscription = getSubscription(reader);
            System.out.println("get the subscription: " + subscription);
            long splitSize = connectorConfig.getSplitSize();
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            while (!reader.hasReachedEndOfTopic()) {
                MessageId startId = reader.readNext().getMessageId();
                System.out.println("Start id:" + startId.toString() + ", waiting for skipping");
                admin.persistentTopics().skipMessages(tableHandle.getTopicName(), subscription, splitSize);
                System.out.println("Skipped" + splitSize + " messages");
                MessageId endId = reader.readNext().getMessageId();
                PulsarSplit split = new PulsarSplit(
                        connectorId,
                        connectorConfig.getServiceUrl(),
                        tableHandle.getTopicName(),
                        tableHandle.getKeyDataFormat(),
                        tableHandle.getMessageDataFormat(),
                        -1,
                        startId,
                        endId);
                splits.add(split);
                System.out.println("New splits:" + startId.toString() + "---" + endId.toString());
            }
            System.out.println("Got all splits");
            return new FixedSplitSource(splits.build());
        }
        catch (MalformedURLException e) {
            throw new PrestoException(PULSAR_ADMIN_ERROR, e);
        }
        catch (PulsarAdminException e) {
            throw new PrestoException(PULSAR_ADMIN_ERROR, e);
        }
        catch (PulsarClientException e) {
            throw new PrestoException(PULSAR_READER_ERROR, e);
        }
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private static String getSubscription(Reader reader)
    {
        try {
            Field field = reader.getClass().getDeclaredField("consumer");
            field.setAccessible(true);
            Object value = field.get(reader);
            field.setAccessible(false);

            if (value == null) {
                return null;
            }
            else if (ConsumerImpl.class.isAssignableFrom(value.getClass())) {
                return ((ConsumerImpl) value).getSubscription();
            }
            throw new PrestoException(PULSAR_READER_ERROR, "could not get correct subscription id of reader");
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new PrestoException(PULSAR_READER_ERROR, "could not get access to the consumer of reader", e);
        }
    }
}
