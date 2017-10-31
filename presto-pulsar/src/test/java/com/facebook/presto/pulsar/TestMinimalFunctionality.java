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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("pulsar")
            .setSchema("default")
            .build();

    private TestingPulsarServer pulsar;
    private String topicName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startPulsar()
            throws Exception
    {
        pulsar = TestingPulsarServer.createStandaloneTestingPulsar();
    }

    @AfterClass(alwaysRun = true)
    public void stopPulsar()
            throws Exception
    {
        pulsar.close();
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        PulsarQueryRunner.createStandalonePulsarQueryRunner(pulsar, queryRunner, new SchemaTableName("default", topicName));
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
    }

    private void createMessages(String topicName, int count)
            throws Exception
    {
        try (Producer producer = pulsar.createProducer(topicName)) {
            for (long i = 0; i < count; i++) {
                Object message = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());
                producer.send(message.toString().getBytes());
            }
        }
    }

    @Test
    public void testTopicExists()
            throws Exception
    {
        QualifiedObjectName name = new QualifiedObjectName("pulsar", "default", topicName);

        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTopicHasData()
            throws Exception
    {
        int count = 1000;
        createMessages(topicName, count);
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + topicName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();

        assertEquals(result, expected);

        createMessages(topicName, count);

        result = queryRunner.execute("SELECT count(1) from " + topicName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count + (long) count)
                .build();

        assertEquals(result, expected);
    }
}
