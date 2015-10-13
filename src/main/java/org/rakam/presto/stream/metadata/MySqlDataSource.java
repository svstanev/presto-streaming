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

import com.google.inject.Inject;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import io.airlift.dbpool.ManagedDataSource;

import javax.sql.PooledConnection;

import java.sql.SQLException;

import static java.lang.String.format;

/**
 * Created by svstanev on 10/13/15.
 */
public class MySqlDataSource extends ManagedDataSource
{
    private final MySqlDataSourceConfig config;
    private MysqlConnectionPoolDataSource dataSource;

    @Inject
    public MySqlDataSource(MySqlDataSourceConfig config)
    {
        super(config.getMaxConnections(), config.getMaxConnectionWait());

        this.config = config;
    }

    private String getJdbcUrl()
    {
        return format(
                "jdbc:mysql://%s:%s/%s",
                this.config.getHost(),
                this.config.getPort(),
                this.config.getDatabaseName());
    }

    @Override
    protected PooledConnection createConnectionInternal() throws SQLException
    {
        if (this.dataSource != null) {
            return this.dataSource.getPooledConnection();
        }

        try {
            MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();

            dataSource.setUrl(this.getJdbcUrl());
            dataSource.setUser(this.config.getUsername());
            dataSource.setPassword(this.config.getPassword());

            dataSource.setConnectTimeout(this.getMaxConnectionWaitMillis());
            dataSource.setInitialTimeout(this.getMaxConnectionWaitMillis());
            dataSource.setDefaultFetchSize(this.config.getDefaultFetchSize());

            PooledConnection connection = dataSource.getPooledConnection();

            this.dataSource = dataSource;

            return connection;
        }
        catch (SQLException e) {
            throw e;
        }
    }
}
