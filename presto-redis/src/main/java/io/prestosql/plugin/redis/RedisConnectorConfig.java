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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.concurrent.TimeUnit.MINUTES;

public class RedisConnectorConfig
{
    private static final int REDIS_DEFAULT_PORT = 6379;

    /**
     * Seed nodes for Redis cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = ImmutableSet.of();

    /**
     * Count parameter for Redis scan command.
     */
    private int redisScanCount = 100;

    /**
     * Get values associated with the specified number of keys in the command such as MGET(key...).
     */
    private int redisMaxKeysPerFetch = 100;

    /**
     * Index of the Redis DB to connect to.
     */
    private int redisDataBaseIndex;

    /**
     * delimiter for separating schema name and table name in the KEY prefix .
     */
    private char redisKeyDelimiter = ':';

    /**
     * password for a password-protected Redis server
     */
    private String redisPassword;

    /**
     * Timeout to connect to Redis.
     */
    private Duration redisConnectTimeout = Duration.valueOf("2000ms");

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of cluster known to this connector.
     */
    private String clusterName;

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given table.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Redis values.
     */
    private File tableDescriptionDir = new File("etc/redis/");

    /**
     * The cache time for redis table description files.
     */
    private Duration tableDescriptionCacheDuration = new Duration(5, MINUTES);

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Whether Redis key string follows "schema:table:*" format
     */
    private boolean keyPrefixSchemaTable;

    /**
     * Whether to use configuration db
     */
    private boolean useConfigDb;

    /**
     * The configuration db url for Redis table.
     */
    private String configDbUrl;

    /**
     * The configuration db user for Redis table.
     */
    private String configDbUser;

    /**
     * The configuration db password for Redis table.
     */
    private String configDbPassword;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("redis.table-description-dir")
    public RedisConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getTableDescriptionCacheDuration()
    {
        return tableDescriptionCacheDuration;
    }

    @Config("redis.table-description-cache-ttl")
    public RedisConnectorConfig setTableDescriptionCacheDuration(Duration tableDescriptionCacheDuration)
    {
        this.tableDescriptionCacheDuration = tableDescriptionCacheDuration;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("redis.table-names")
    public RedisConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getClusterName()
    {
        return clusterName;
    }

    @Config("redis.cluster-name")
    public RedisConnectorConfig setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("redis.default-schema")
    public RedisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("redis.nodes")
    public RedisConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    public int getRedisScanCount()
    {
        return redisScanCount;
    }

    @Config("redis.scan-count")
    public RedisConnectorConfig setRedisScanCount(int redisScanCount)
    {
        this.redisScanCount = redisScanCount;
        return this;
    }

    @Min(1)
    public int getRedisMaxKeysPerFetch()
    {
        return redisMaxKeysPerFetch;
    }

    @Config("redis.max-keys-per-fetch")
    public RedisConnectorConfig setRedisMaxKeysPerFetch(int redisMaxKeysPerFetch)
    {
        this.redisMaxKeysPerFetch = redisMaxKeysPerFetch;
        return this;
    }

    public int getRedisDataBaseIndex()
    {
        return redisDataBaseIndex;
    }

    @Config("redis.database-index")
    public RedisConnectorConfig setRedisDataBaseIndex(int redisDataBaseIndex)
    {
        this.redisDataBaseIndex = redisDataBaseIndex;
        return this;
    }

    @MinDuration("1s")
    public Duration getRedisConnectTimeout()
    {
        return redisConnectTimeout;
    }

    @Config("redis.connect-timeout")
    public RedisConnectorConfig setRedisConnectTimeout(String redisConnectTimeout)
    {
        this.redisConnectTimeout = Duration.valueOf(redisConnectTimeout);
        return this;
    }

    public char getRedisKeyDelimiter()
    {
        return redisKeyDelimiter;
    }

    @Config("redis.key-delimiter")
    public RedisConnectorConfig setRedisKeyDelimiter(String redisKeyDelimiter)
    {
        this.redisKeyDelimiter = redisKeyDelimiter.charAt(0);
        return this;
    }

    public String getRedisPassword()
    {
        return redisPassword;
    }

    @Config("redis.password")
    @ConfigSecuritySensitive
    public RedisConnectorConfig setRedisPassword(String redisPassword)
    {
        this.redisPassword = redisPassword;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("redis.hide-internal-columns")
    public RedisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public boolean isKeyPrefixSchemaTable()
    {
        return keyPrefixSchemaTable;
    }

    @Config("redis.key-prefix-schema-table")
    public RedisConnectorConfig setKeyPrefixSchemaTable(boolean keyPrefixSchemaTable)
    {
        this.keyPrefixSchemaTable = keyPrefixSchemaTable;
        return this;
    }

    public boolean isUseConfigDb()
    {
        return useConfigDb;
    }

    @Config("redis.use-config-db")
    public RedisConnectorConfig setUseConfigDb(boolean useConfigDb)
    {
        this.useConfigDb = useConfigDb;
        return this;
    }

    public String getConfigDbUrl()
    {
        return configDbUrl;
    }

    @Config("redis.config-db-url")
    public RedisConnectorConfig setConfigDbUrl(String configDbUrl)
    {
        this.configDbUrl = configDbUrl;
        return this;
    }

    public String getConfigDbUser()
    {
        return configDbUser;
    }

    @Config("redis.config-db-user")
    public RedisConnectorConfig setConfigDbUser(String configDbUser)
    {
        this.configDbUser = configDbUser;
        return this;
    }

    public String getConfigDbPassword()
    {
        return configDbPassword;
    }

    @Config("redis.config-db-password")
    public RedisConnectorConfig setConfigDbPassword(String configDbPassword)
    {
        this.configDbPassword = configDbPassword;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();

        return StreamSupport.stream(splitter.split(nodes).spliterator(), false)
                .map(RedisConnectorConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(REDIS_DEFAULT_PORT);
    }
}
