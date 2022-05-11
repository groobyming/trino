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
package io.prestosql.plugin.redis;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class RedisTableConfigDaoProvider
        implements Provider<RedisTableConfigDao>
{
    private final RedisTableConfigDao dao;

    @Inject
    public RedisTableConfigDaoProvider(RedisConnectorConfig config)
    {
        requireNonNull(config, "DbCatalogConfig is null");
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(requireNonNull(config.getConfigDbUrl(), "redis.config-db-url is null"));
        dataSource.setUser(requireNonNull(config.getConfigDbUser(), "redis.config-db-user is null"));
        dataSource.setPassword(requireNonNull(config.getConfigDbPassword(), "redis.config-db-password is null"));
        this.dao = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .onDemand(RedisTableConfigDao.class);
    }

    @Override
    public RedisTableConfigDao get()
    {
        return dao;
    }
}
