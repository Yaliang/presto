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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.AbstractMap;
import java.util.Map;

public class PulsarQueryRunner
{
    private static final Logger log = Logger.get(PulsarQueryRunner.class);

    private PulsarQueryRunner()
    {
    }

    public static void createStandalonePulsarQueryRunner(TestingPulsarServer pulsar, StandaloneQueryRunner queryRunner, SchemaTableName schemaTableName)
            throws Exception
    {
        PulsarPlugin pulsarPlugin = new PulsarPlugin();
        Map<SchemaTableName, PulsarTopicDescription> supplier = ImmutableMap.<SchemaTableName, PulsarTopicDescription>builder().put(createEmptyTopicDescription(schemaTableName)).build();
        pulsarPlugin.setTableDescriptionSupplier(() -> supplier);
        queryRunner.installPlugin(pulsarPlugin);

        Map<String, String> pulsarConfig = ImmutableMap.of(
                "pulsar.service-url", pulsar.getBrokerServiceUrl(),
                "pulsar.web-service-url", pulsar.getBrokerWebServiceUrl(),
                "pulsar.table-names", Joiner.on(",").join(supplier.keySet()),
                "pulsar.reader-operation-timeout", "120s",
                "pulsar.default-schema", "default");
        queryRunner.createCatalog("pulsar", "pulsar", pulsarConfig);
    }

    private static Map.Entry<SchemaTableName, PulsarTopicDescription> createEmptyTopicDescription(SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new PulsarTopicDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), "persistent://prop/use/ns-abc/" + schemaTableName.getTableName(), null, null));
    }
}
