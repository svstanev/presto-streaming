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

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/01/15 13:47.
 */
public class SimpleStreamRecordSink implements ConnectorPageSink
{
    private final SimpleRowTable table;

    public SimpleStreamRecordSink(SimpleRowTable table)
    {
        this.table = table;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        for (Accumulator accumulator : table.getAggregations()) {
            accumulator.addInput(page);
        }
    }

    @Override
    public Collection<Slice> commit()
    {
        ImmutableList.Builder<Slice> fragments = ImmutableList.builder();
        return fragments.build();
    }

    @Override
    public void rollback()
    {
        throw new UnsupportedOperationException();
    }
}
