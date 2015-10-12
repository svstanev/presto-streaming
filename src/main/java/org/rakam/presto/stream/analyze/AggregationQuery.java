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

package org.rakam.presto.stream.analyze;

import com.facebook.presto.sql.tree.SortItem;
import org.rakam.presto.stream.analyze.QueryAnalyzer.AggregationField;
import org.rakam.presto.stream.analyze.QueryAnalyzer.GroupByField;

import java.util.List;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/01/15 15:42.
 */
public class AggregationQuery
{
    public final Optional<String> limit;
    public final List<SortItem> orderBy;
    public final List<AggregationField> aggregationFields;
    public final List<GroupByField> groupByFields;

    public AggregationQuery(List<AggregationField> aggregationFields, List<GroupByField> groupByFields, List<SortItem> orderBy, Optional<String> limit)
    {
        this.limit = limit;
        this.orderBy = orderBy;
        this.aggregationFields = aggregationFields;
        this.groupByFields = groupByFields;
    }

    // since group by fields are required, if this is a group by query, there is at least one group by column.
    public boolean isGroupByQuery()
    {
        return !groupByFields.isEmpty();
    }
}
