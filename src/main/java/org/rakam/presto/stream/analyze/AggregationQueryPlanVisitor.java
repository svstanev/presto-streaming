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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/01/15 15:40.
 */
class AggregationQueryPlanVisitor extends PlanVisitor<Void, Void>
{
    Map<Symbol, Signature> aggregationFields = new HashMap<>();
    List<String> fields = new ArrayList<>();
    List<Symbol> fieldSymbols;

    @Override
    protected Void visitPlan(PlanNode node, Void context)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "operation is not supported.");
    }

    @Override
    public Void visitAggregation(AggregationNode node, Void context)
    {
        Map<Symbol, Signature> functionMap = node.getFunctions();
        Map<Symbol, FunctionCall> aggregationSymbols = node.getAggregations();
        for (Map.Entry<Symbol, FunctionCall> entry : aggregationSymbols.entrySet()) {
            Signature signature = functionMap.get(entry.getKey());
            node.getGroupBy();
            aggregationFields.put(entry.getKey(), signature);
        }

        return null;
    }

    @Override
    public Void visitOutput(OutputNode node, Void context)
    {
        fields = node.getColumnNames();
        fieldSymbols = node.getOutputSymbols();
        return null;
    }
}
