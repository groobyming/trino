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
package io.trino.plugin.atop;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.atop.AtopConnectorConfig.AtopSecurity;
import io.trino.plugin.base.security.AllowAllAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static java.util.Objects.requireNonNull;

public class AtopConnectorFactory
        implements ConnectorFactory
{
    private final Class<? extends AtopFactory> atopFactoryClass;
    private final ClassLoader classLoader;

    public AtopConnectorFactory(Class<? extends AtopFactory> atopFactoryClass, ClassLoader classLoader)
    {
        this.atopFactoryClass = requireNonNull(atopFactoryClass, "atopFactoryClass is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "atop";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new AtopHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new AtopModule(
                            atopFactoryClass,
                            context.getTypeManager(),
                            context.getNodeManager(),
                            context.getNodeManager().getEnvironment(),
                            catalogName),
                    conditionalModule(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity() == AtopSecurity.NONE,
                            new AllowAllAccessControlModule()),
                    conditionalModule(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity() == AtopSecurity.FILE,
                            binder -> {
                                binder.install(new FileBasedAccessControlModule(catalogName));
                                binder.install(new JsonModule());
                            }));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(AtopConnector.class);
        }
    }
}
