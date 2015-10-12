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

package org.rakam.presto.stream.storage;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.google.inject.Inject;

import static com.facebook.presto.util.Types.checkType;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/01/15 13:45.
 */
public class StreamPageSinkProvider implements ConnectorPageSinkProvider
{
    private final StreamStorageManager storageManager;

    @Inject
    public StreamPageSinkProvider(StreamStorageManager storageManager)
    {
        this.storageManager = storageManager;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        StreamInsertTableHandle handle = checkType(tableHandle, StreamInsertTableHandle.class, "tableHandle");

        MaterializedView materializedView = storageManager.get(handle.getTableId());
        if (materializedView instanceof SimpleRowTable) {
            return new SimpleStreamRecordSink((SimpleRowTable) materializedView);
        }
        else {
//            StreamMultipleRowRecordSink recordSink = new StreamMultipleRowRecordSink((MultipleRowTable) materializedView);
//            return new RecordPageSink(recordSink);
            return new GroupByStreamRecordSink((GroupByRowTable) materializedView);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        StreamInsertTableHandle handle = checkType(insertTableHandle, StreamInsertTableHandle.class, "tableHandle");

        MaterializedView materializedView = storageManager.get(handle.getTableId());
        if (materializedView instanceof SimpleRowTable) {
            return new SimpleStreamRecordSink((SimpleRowTable) materializedView);
        }
        else {
//            StreamMultipleRowRecordSink recordSink = new StreamMultipleRowRecordSink((MultipleRowTable) materializedView);
//            return new RecordPageSink(recordSink);
            return new GroupByStreamRecordSink((GroupByRowTable) materializedView);
        }
    }
}
