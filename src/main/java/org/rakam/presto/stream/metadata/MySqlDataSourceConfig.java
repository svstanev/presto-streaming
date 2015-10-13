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

package org.rakam.presto.stream.metadata;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by svstanev on 10/13/15.
 */
public class MySqlDataSourceConfig
{
    private String host;
    private String port;
    private String databaseName;
    private String username;
    private String password;
    private int maxConnections = 10;
    private Duration maxConnectionWait = new Duration(500, TimeUnit.MILLISECONDS);
    private int defaultFetchSize = 100;

    public String getHost()
    {
        return this.host;
    }

    @Config("metadata.db.host")
    public MySqlDataSourceConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    public String getPort()
    {
        return this.port;
    }

    @Config("metadata.db.port")
    public MySqlDataSourceConfig setPort(String port)
    {
        this.port = port;
        return this;
    }

    public String getDatabaseName()
    {
        return this.databaseName;
    }

    @Config("metadata.db.database")
    public MySqlDataSourceConfig setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    public String getUsername()
    {
        return this.username;
    }

    @Config("metadata.db.username")
    public MySqlDataSourceConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return this.password;
    }

    @Config("metadata.db.password")
    public MySqlDataSourceConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public int getDefaultFetchSize()
    {
        return this.defaultFetchSize;
    }

    @Config("metadata.db.fetch-size")
    public MySqlDataSourceConfig setDefaultFetchSize(int defaultFetchSize)
    {
        this.defaultFetchSize = defaultFetchSize;
        return this;
    }

    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("metadata.db.max-connections")
    public MySqlDataSourceConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;

    }

    public Duration getMaxConnectionWait()
    {
        return this.maxConnectionWait;
    }

    /**
     * Sets the maximum time a client is allowed to wait before a connection. If
     * a connection can not be obtained within the limit, a {@link
     * SqlTimeoutException} is thrown.
     */
    @Config("metadata.db.max-connection-wait")
    public MySqlDataSourceConfig setMaxConnectionWait(Duration maxConnectionWait)
    {
        this.maxConnectionWait = maxConnectionWait;
        return this;
    }
}
