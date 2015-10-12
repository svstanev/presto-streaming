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
package org.rakam.presto.stream;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import org.rakam.presto.stream.metadata.StreamMetadata;
import org.rakam.presto.stream.query.StreamRecordSetProvider;
import org.rakam.presto.stream.storage.StreamPageSinkProvider;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class StreamConnector
        implements Connector
{
    private final StreamMetadata metadata;
    private final StreamSplitManager splitManager;
    private final StreamRecordSetProvider recordSetProvider;
    private final StreamHandleResolver handleResolver;
    private final StreamPageSinkProvider pageSinkProvider;

    @Inject
    public StreamConnector(
            StreamMetadata metadata,
            StreamSplitManager splitManager,
            StreamRecordSetProvider recordSetProvider,
            StreamPageSinkProvider pageSinkProvider,
            StreamHandleResolver handleResolver)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }
}
