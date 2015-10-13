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
import com.facebook.presto.spi.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import org.rakam.presto.stream.metadata.DatabaseMetadataModule;
import org.rakam.presto.stream.util.RebindSafeMBeanServer;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public class StreamConnectorFactory
        implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final String name;
    private final Module module;

    public StreamConnectorFactory(
            String name,
            Module module,
            Map<String, String> optionalConfig)
    {
        this.name = requireNonNull(name, "name is null");
        this.module = requireNonNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try {
            Bootstrap app = new Bootstrap(
                    new StreamModule(connectorId),
                    new MBeanModule(),
                    new DatabaseMetadataModule(),
                    this.module,
                    binder -> {
                        MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());

                        binder.bind(MBeanServer.class).toInstance(mbeanServer);
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(this.optionalConfig)
                    .initialize();

            return injector.getInstance(StreamConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
