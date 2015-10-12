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
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.google.common.primitives.Ints;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 08:16.
 */
public class GroupByRowTable implements MaterializedView
{
    private final GroupedAccumulator[] groupedAggregations;
    private final GroupByHash groupByHash;
    private final boolean[] aggregationChannels;

    public GroupByRowTable(GroupedAccumulator[] groupedAggregations, GroupByHash groupByHash, int[] groupByHashChannels)
    {
        this.groupedAggregations = groupedAggregations;
        this.groupByHash = groupByHash;
        boolean[] booleans = new boolean[groupedAggregations.length + groupByHashChannels.length];
        for (int i = 0; i < booleans.length; i++) {
            booleans[i] = !Ints.contains(groupByHashChannels, i);
        }
        this.aggregationChannels = booleans;
    }

    public boolean isAggregationChannel(int channel)
    {
        return aggregationChannels[channel];
    }

    public GroupByHash getGroupByHash()
    {
        return groupByHash;
    }

    public GroupedAccumulator[] getGroupedAggregations()
    {
        return groupedAggregations;
    }
}
