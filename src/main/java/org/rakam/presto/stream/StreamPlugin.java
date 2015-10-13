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

import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.discovery.client.ServiceSelectorFactory;
import org.rakam.presto.stream.util.CurrentNodeId;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;

import static java.util.Objects.requireNonNull;

public class StreamPlugin
        implements Plugin
{
    private final String name;
    private TypeManager typeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private NodeManager nodeManager;
    private MetadataManager metadataManager;
    private List<PlanOptimizer> planOptimizers;
    private FeaturesConfig featuresConfig;
    private SqlParser sqlParser;
    private AccessControl accessControl;
    private Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;
    private ServiceSelectorFactory serviceSelectorFactory;

    public StreamPlugin()
    {
        this(getPluginInfo());
    }

    public StreamPlugin(PluginInfo info)
    {
        this(info.getName());
    }

    public StreamPlugin(String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");

        this.name = name;
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = StreamPlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return this.optionalConfig;
    }

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(
                    new StreamConnectorFactory(
                            this.name,
                            new ExtDependenciesModule(this),    //this.module,
                            this.getOptionalConfig()
                    )));
        }
        return ImmutableList.of();
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Inject
    public void setMetadataManager(MetadataManager metadataManager)
    {
        this.metadataManager = metadataManager;
    }

    @Inject
    public void setPlanOptimizers(List<PlanOptimizer> metadataManager)
    {
        this.planOptimizers = metadataManager;
    }

    @Inject
    public void setFeaturesConfig(FeaturesConfig featuresConfig)
    {
        this.featuresConfig = featuresConfig;
    }

    @Inject
    public void setSqlParser(SqlParser sqlParser)
    {
        this.sqlParser = sqlParser;
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Inject
    public synchronized void setAccessControl(AccessControl accessControl)
    {
        this.accessControl = accessControl;
    }

    @Inject
    public synchronized void setTasks(Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks)
    {
        this.tasks = tasks;
    }

    @Inject
    public synchronized void setServiceSelectorFactory(ServiceSelectorFactory serviceSelectorFactory)
    {
        this.serviceSelectorFactory = serviceSelectorFactory;
    }

    class ExtDependenciesModule implements Module
    {
        private final StreamPlugin owner;

        ExtDependenciesModule(StreamPlugin owner)
        {
            this.owner = owner;
        }

        @Override
        public void configure(Binder binder)
        {
            CurrentNodeId currentNodeId = new CurrentNodeId(this.owner.nodeManager.getCurrentNode().getNodeIdentifier());

            binder.bind(CurrentNodeId.class).toInstance(currentNodeId);
            binder.bind(NodeManager.class).toInstance(this.owner.nodeManager);
            binder.bind(TypeManager.class).toInstance(this.owner.typeManager);
            binder.bind(MetadataManager.class).toInstance(this.owner.metadataManager);
            binder.bind(SqlParser.class).toInstance(this.owner.sqlParser);
            binder.bind(FeaturesConfig.class).toInstance(this.owner.featuresConfig);
            binder.bind(AccessControl.class).toInstance(this.owner.accessControl);
            binder.bind(ServiceSelectorFactory.class).toInstance(this.owner.serviceSelectorFactory);

            binder.bind(new TypeLiteral<Map<Class<? extends Statement>, DataDefinitionTask<?>>>()
            {
            }).toInstance(this.owner.tasks);

            binder.bind(new TypeLiteral<List<PlanOptimizer>>()
            {
            }).toInstance(this.owner.planOptimizers);
        }
    }
}
