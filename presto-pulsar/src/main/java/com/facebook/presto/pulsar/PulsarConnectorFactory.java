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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Creates Pulsar Connectors based off connectorId and specific configuration.
 */
public class PulsarConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Supplier<Map<SchemaTableName, PulsarTopicDescription>>> tableDescriptionSupplier;

    PulsarConnectorFactory(Optional<Supplier<Map<SchemaTableName, PulsarTopicDescription>>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "pulsar";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PulsarHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PulsarConnectorModule(),
                    binder -> {
                        binder.bind(PulsarConnectorId.class).toInstance(new PulsarConnectorId(connectorId));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());

                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, PulsarTopicDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                        }
                        else {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, PulsarTopicDescription>>>() {}).to(PulsarTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    });

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(PulsarConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
