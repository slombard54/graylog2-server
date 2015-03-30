/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.shared.initializers;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import me.bazhenov.groovysh.GroovyShellService;
import org.graylog2.plugin.BaseConfiguration;
import org.graylog2.shared.bindings.InstantiationService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;

@Singleton
public class GroovyShellSetupService extends AbstractIdleService {
    private final GroovyShellService groovyShellService = new GroovyShellService();
    private final BaseConfiguration configuration;

    @Inject
    public GroovyShellSetupService(InstantiationService instantiationService,
                                   BaseConfiguration configuration) {
        this.configuration = configuration;
        groovyShellService.setPort(configuration.getGroovyShellPort());

        Map<String, Object> binding = Maps.newHashMap();
        binding.put("instantiationService", instantiationService);
        groovyShellService.setBindings(binding);
    }

    @Override
    protected void startUp() throws Exception {
        if (configuration.isGroovyShellEnable()) {
            groovyShellService.start();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        if(configuration.isGroovyShellEnable()) {
            groovyShellService.destroy();
        }
    }
}
