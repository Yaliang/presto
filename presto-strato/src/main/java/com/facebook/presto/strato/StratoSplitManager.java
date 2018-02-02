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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.strato.StratoErrorCode.STRATO_SPLIT_ERROR;
import static java.util.Objects.requireNonNull;

public class StratoSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;

    @Inject
    public StratoSplitManager(StratoConnectorId connectorId, StratoMetadata metadata)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        StratoTableLayoutHandle layoutHandle = (StratoTableLayoutHandle) layout;
        StratoTableHandle tableHandle = layoutHandle.getTable();
        List<ConnectorSplit> splits = new ArrayList<>();
        try {
            URI uri = new URI(layoutHandle.getFormattedUrl());
            splits.add(new StratoSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri, layoutHandle.getQueryMap()));
        }
        catch (Exception e) {
            throw new PrestoException(STRATO_SPLIT_ERROR, "Invalid strato path: " + layoutHandle.getFormattedUrl(), e);
        }

        return new FixedSplitSource(splits);
    }
}
