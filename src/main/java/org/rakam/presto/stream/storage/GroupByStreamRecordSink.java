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

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 11:23.
 */
public class GroupByStreamRecordSink implements ConnectorPageSink
{
    GroupByRowTable table;

    public GroupByStreamRecordSink(GroupByRowTable table)
    {
        this.table = table;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        GroupByHash groupByHash = table.getGroupByHash();
        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

        GroupedAccumulator[] groupedAggregations = table.getGroupedAggregations();

        int aggregationIdx = 0;
        int channelCount = page.getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            if (table.isAggregationChannel(i)) {
                Block block = page.getBlock(i);
                groupedAggregations[aggregationIdx++].addIntermediate(groupIds, block);
            }
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
