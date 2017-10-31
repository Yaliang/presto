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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class PulsarConnectorConfig
{
    private static final int PULSAR_DEFAULT_PORT = 6650;

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Pulsar topics.
     */
    private File tableDescriptionDir = new File("etc/pulsar/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    private String webServiceUrl = "";
    private String serviceUrl = "";
    private int pulsarQueueSize = 1_000;
    private int splitSize = 10_000;
    private boolean adminUseTls = false;
    private boolean adminTlsAllowInsecureConnection = false;
    private String adminAuthPluginClassName = "";
    private String adminAuthParams = "";
    private String adminTlsTrustCertsFilePath = "";

    private boolean readerUseTls = false;
    private boolean readerTlsAllowInsecureConnection = false;
    private String readerAuthPluginClassName = "";
    private String readerAuthParams = "";
    private String readerTlsTrustCertsFilePath = "";
    private int connectionsPerBroker = 1;
    private int numIoThreads = 1;
    private int numListenerThreads = 1;
    private Duration operationTimeout = Duration.valueOf("30s");

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("pulsar.table-description-dir")
    public PulsarConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("pulsar.table-names")
    public PulsarConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("pulsar.default-schema")
    public PulsarConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    public String getServiceUrl()
    {
        return serviceUrl;
    }

    @Config("pulsar.service-url")
    public PulsarConnectorConfig setServiceUrl(String serviceUrl)
    {
        this.serviceUrl = serviceUrl;
        return this;
    }

    @NotNull
    public String getWebServiceUrl()
    {
        return webServiceUrl;
    }

    @Config("pulsar.web-service-url")
    public PulsarConnectorConfig setWebServiceUrl(String webServiceUrl)
    {
        this.webServiceUrl = webServiceUrl;
        return this;
    }

    public int getSplitSize()
    {
        return splitSize;
    }

    @Config("pulsar.split-size")
    public PulsarConnectorConfig setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
        return this;
    }

    public int getPulsarQueueSize()
    {
        return pulsarQueueSize;
    }

    @Config("pulsar.queue-size")
    public PulsarConnectorConfig setPulsarQueueSize(int pulsarQueueSize)
    {
        this.pulsarQueueSize = pulsarQueueSize;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("pulsar.hide-internal-columns")
    public PulsarConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public boolean isAdminUseTls()
    {
        return adminUseTls;
    }

    @Config("pulsar.admin-use-tls")
    public PulsarConnectorConfig setAdminUseTls(boolean adminUseTls)
    {
        this.adminUseTls = adminUseTls;
        return this;
    }

    public boolean isAdminTlsAllowInsecureConnection()
    {
        return adminTlsAllowInsecureConnection;
    }

    @Config("pulsar.admin-tls-allow-insecure-connection")
    public PulsarConnectorConfig setAdminTlsAllowInsecureConnection(boolean adminTlsAllowInsecureConnection)
    {
        this.adminTlsAllowInsecureConnection = adminTlsAllowInsecureConnection;
        return this;
    }

    @NotNull
    public String getAdminAuthPluginClassName()
    {
        return adminAuthPluginClassName;
    }

    @Config("pulsar.admin-auth-plugin-class-name")
    public PulsarConnectorConfig setAdminAuthPluginClassName(String adminAuthPluginClassName)
    {
        this.adminAuthPluginClassName = adminAuthPluginClassName;
        return this;
    }

    @NotNull
    public String getAdminAuthParams()
    {
        return adminAuthParams;
    }

    @Config("pulsar.admin-auth-params")
    public PulsarConnectorConfig setAdminAuthParams(String adminAuthParams)
    {
        this.adminAuthParams = adminAuthParams;
        return this;
    }

    @NotNull
    public String getAdminTlsTrustCertsFilePath()
    {
        return adminTlsTrustCertsFilePath;
    }

    @Config("pulsar.admin-tls-trust-certs-file-path")
    public PulsarConnectorConfig setAdminTlsTrustCertsFilePath(String adminTlsTrustCertsFilePath)
    {
        this.adminTlsTrustCertsFilePath = adminTlsTrustCertsFilePath;
        return this;
    }

    public boolean isReaderUseTls()
    {
        return readerUseTls;
    }

    @Config("pulsar.reader-use-tls")
    public PulsarConnectorConfig setReaderUseTls(boolean readerUseTls)
    {
        this.readerUseTls = readerUseTls;
        return this;
    }

    public boolean isReaderTlsAllowInsecureConnection()
    {
        return readerTlsAllowInsecureConnection;
    }

    @Config("pulsar.reader-tls-allow-insecure-connection")
    public PulsarConnectorConfig setReaderTlsAllowInsecureConnection(boolean readerTlsAllowInsecureConnection)
    {
        this.readerTlsAllowInsecureConnection = readerTlsAllowInsecureConnection;
        return this;
    }

    @NotNull
    public String getReaderAuthPluginClassName()
    {
        return readerAuthPluginClassName;
    }

    @Config("pulsar.reader-auth-plugin-class-name")
    public PulsarConnectorConfig setReaderAuthPluginClassName(String readerAuthPluginClassName)
    {
        this.readerAuthPluginClassName = readerAuthPluginClassName;
        return this;
    }

    @NotNull
    public String getReaderAuthParams()
    {
        return readerAuthParams;
    }

    @Config("pulsar.reader-auth-params")
    public PulsarConnectorConfig setReaderAuthParams(String readerAuthParams)
    {
        this.readerAuthParams = readerAuthParams;
        return this;
    }

    @NotNull
    public String getReaderTlsTrustCertsFilePath()
    {
        return readerTlsTrustCertsFilePath;
    }

    @Config("pulsar.reader-tls-trust-certs-file-path")
    public PulsarConnectorConfig setReaderTlsTrustCertsFilePath(String readerTlsTrustCertsFilePath)
    {
        this.readerTlsTrustCertsFilePath = readerTlsTrustCertsFilePath;
        return this;
    }

    @Min(1)
    public int getConnectionsPerBroker()
    {
        return connectionsPerBroker;
    }

    @Config("pulsar.reader-max-connections-per-broker")
    public PulsarConnectorConfig setConnectionsPerBroker(int connectionsPerBroker)
    {
        this.connectionsPerBroker = connectionsPerBroker;
        return this;
    }

    @Min(1)
    public int getNumIoThreads()
    {
        return numIoThreads;
    }

    @Config("pulsar.reader-max-io-threads")
    public PulsarConnectorConfig setNumIoThreads(int numIoThreads)
    {
        this.numIoThreads = numIoThreads;
        return this;
    }

    @Min(1)
    public int getNumListenerThreads()
    {
        return numListenerThreads;
    }

    @Config("pulsar.reader-max-listener-threads")
    public PulsarConnectorConfig setNumListenerThreads(int numListenerThreads)
    {
        this.numListenerThreads = numListenerThreads;
        return this;
    }

    @MinDuration("1ms")
    public Duration getOperationTimeout()
    {
        return operationTimeout;
    }

    @Config("pulsar.reader-operation-timeout")
    public PulsarConnectorConfig setOperationTimeout(Duration operationTimeout)
    {
        this.operationTimeout = operationTimeout;
        return this;
    }
}
