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

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Producer;

public class TestingPulsarServer
        extends BrokerTestBase
{
    public static TestingPulsarServer createStandaloneTestingPulsar()
            throws Exception
    {
        TestingPulsarServer pulsar = new TestingPulsarServer();
        pulsar.setup();
        return pulsar;
    }

    public void close()
            throws Exception
    {
        cleanup();
    }

    public Producer createProducer(String topicName)
            throws Exception
    {
        return this.pulsarClient.createProducer("persistent://prop/use/ns-abc/" + topicName);
    }

    public String getBrokerServiceUrl()
    {
        return "pulsar://localhost:" + this.BROKER_PORT;
    }

    public String getBrokerWebServiceUrl()
    {
        return "http://localhost:" + this.BROKER_WEBSERVICE_PORT;
    }

    @Override
    public void setup()
            throws Exception
    {
        super.baseSetup();
    }

    @Override
    public void cleanup()
            throws Exception
    {
        super.internalCleanup();
    }
}
