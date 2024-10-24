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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.util.Optional;

import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;

public class TestIgniteDistributeQueries
        extends AbstractTestDistributedQueries
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = new TestingIgniteServer();
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy() throws IOException
    {
        igniteServer.close();
        igniteServer = null;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                igniteServer::execute,
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    public void testCommentTable()
    {
        // SQLServer connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("varbinary")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
