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

import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.impl.PulsarClientImpl;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PulsarClientManager
{
    private static final Logger log = Logger.get(PulsarClientManager.class);

    private final LoadingCache<String, PulsarClient> clientCache;
    private final LoadingCache<URL, PulsarAdmin> adminCache;

    private final String connectorId;
    private final NodeManager nodeManager;
    private final PulsarConnectorConfig connectorConfig;

    @Inject
    public PulsarClientManager(
            PulsarConnectorId connectorId,
            PulsarConnectorConfig connectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorConfig = requireNonNull(connectorConfig, "connectorConfig is null");
        this.clientCache = CacheBuilder.newBuilder().build(new PulsarClientCacheLoader());
        this.adminCache = CacheBuilder.newBuilder().build(new PulsarAdminCacheLoader());
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<String, PulsarClient> entry : clientCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing client %s:", entry.getKey());
            }
        }
        for (Map.Entry<URL, PulsarAdmin> entry : adminCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing admin client %s:", entry.getKey());
            }
        }
    }

    public PulsarClient getClient(String serviceUrl)
    {
        requireNonNull(serviceUrl, "serviceUrl is null");
        try {
            return clientCache.get(serviceUrl);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public Reader getReader(String serviceUrl, String topic, MessageId startMessage, int receiverQueueSize)
    {
        requireNonNull(serviceUrl, "serviceUrl is null");
        try {
            return clientCache.get(serviceUrl).createReader(topic, startMessage, new ReaderConfiguration().setReceiverQueueSize(receiverQueueSize));
        }
        catch (ExecutionException | PulsarClientException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public Reader getReader(String serviceUrl, String topic, MessageId startMessage)
    {
        requireNonNull(serviceUrl, "serviceUrl is null");
        try {
            return clientCache.get(serviceUrl).createReader(topic, startMessage, new ReaderConfiguration());
        }
        catch (ExecutionException | PulsarClientException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public PulsarAdmin getAdmin(URL serviceUrl)
    {
        requireNonNull(serviceUrl, "serviceUrl is null");
        try {
            return adminCache.get(serviceUrl);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class PulsarClientCacheLoader
            extends CacheLoader<String, PulsarClient>
    {
        @Override
        public PulsarClient load(String serviceUrl)
                throws Exception
        {
            log.info("Creating new Pulsar client for %s", serviceUrl);
            ClientConfiguration config = new ClientConfiguration();
            config.setAuthentication(connectorConfig.getReaderAuthPluginClassName(), connectorConfig.getReaderAuthParams());
            config.setUseTls(connectorConfig.isReaderUseTls());
            config.setTlsAllowInsecureConnection(connectorConfig.isReaderTlsAllowInsecureConnection());
            config.setTlsTrustCertsFilePath(connectorConfig.getReaderTlsTrustCertsFilePath());
            config.setOperationTimeout(toIntExact(connectorConfig.getOperationTimeout().toMillis()), connectorConfig.getOperationTimeout().getUnit());
            config.setConnectionsPerBroker(connectorConfig.getConnectionsPerBroker());
            config.setIoThreads(connectorConfig.getNumIoThreads());
            config.setListenerThreads(connectorConfig.getNumListenerThreads());

            return new PulsarClientImpl(serviceUrl, config);
        }
    }

    private class PulsarAdminCacheLoader
            extends CacheLoader<URL, PulsarAdmin>
    {
        @Override
        public PulsarAdmin load(URL serviceUrl)
                throws Exception
        {
            log.info("Creating new Pulsar admin client for %s", serviceUrl.toString());
            ClientConfiguration config = new ClientConfiguration();
            config.setAuthentication(connectorConfig.getAdminAuthPluginClassName(), connectorConfig.getAdminAuthParams());
            config.setUseTls(connectorConfig.isAdminUseTls());
            config.setTlsAllowInsecureConnection(connectorConfig.isAdminTlsAllowInsecureConnection());
            config.setTlsTrustCertsFilePath(connectorConfig.getAdminTlsTrustCertsFilePath());

            return new PulsarAdmin(serviceUrl, config);
        }
    }
}
