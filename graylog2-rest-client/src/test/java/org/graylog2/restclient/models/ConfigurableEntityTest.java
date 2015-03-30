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
package org.graylog2.restclient.models;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.graylog2.rest.models.configuration.responses.RequestedConfigurationField;
import org.graylog2.rest.models.configuration.responses.TextField;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurableEntityTest {
    @Spy
    private ConfigurableEntity configurableEntity;
    private final Map<String, Object> configuration = Maps.newHashMap();
    private final List<RequestedConfigurationField> requestedConfigurationFields = Lists.newArrayList();

    @Before
    public void setUp() throws Exception {
        when(configurableEntity.getConfiguration()).thenReturn(configuration);
    }

    @Test
    public void testMaskingPasswordField() throws Exception {
        final String passwordField = "password";
        configuration.put(passwordField, "foobar");
        addPasswordField(passwordField);

        final Map<String, Object> result = configurableEntity.getConfiguration(requestedConfigurationFields);

        assertEquals("Password should be masked in result!", result.get(passwordField), "*******");
    }

    @Test
    public void testNotStubbingNonPasswordField() throws Exception {
        final String userField = "username";
        final String username = "johndoe";
        configuration.put(userField, username);
        addNonPasswordField(userField);

        final Map<String, Object> result = configurableEntity.getConfiguration(requestedConfigurationFields);

        assertEquals("Non-password field should not be masked in result!", result.get(userField), username);
    }

    @Test
    public void testMixedPasswordAndNonPasswordFields() throws Exception {
        final String passwordField = "password";
        configuration.put(passwordField, "foobar");
        addPasswordField(passwordField);

        final String userField = "username";
        final String username = "johndoe";
        configuration.put(userField, username);
        addNonPasswordField(userField);

        final Map<String, Object> result = configurableEntity.getConfiguration(requestedConfigurationFields);

        assertEquals("Non-password field should not be masked in result!", result.get(userField), username);
        assertEquals("Password should be masked in result!", result.get(passwordField), "*******");
    }

    private void addNonPasswordField(String userField) {
        final Map.Entry<String, Map<String, Object>> textField = stubConfigurationFieldConfig("text", userField, new ArrayList<String>(), false);

        requestedConfigurationFields.add(new TextField(textField));
    }

    private void addPasswordField(String passwordFieldName) {
        final Map.Entry<String, Map<String, Object>> passwordTextField = stubConfigurationFieldConfig("text", passwordFieldName, new ArrayList<String>() {{
            add("is_password");
        }}, false);

        requestedConfigurationFields.add(new TextField(passwordTextField));
    }

    private Map.Entry<String, Map<String, Object>> stubConfigurationFieldConfig(final String type, final String title, final List<String> attributes, final Boolean isOptional) {
        return new Map.Entry<String, Map<String, Object>>() {
            @Override
            public String getKey() {
                return title;
            }

            @Override
            public Map<String, Object> getValue() {
                return new HashMap<String, Object>() {{
                    put("type", type);
                    put("attributes", attributes);
                    put("is_optional", isOptional);
                }};
            }

            @Override
            public Map<String, Object> setValue(Map<String, Object> value) {
                return null;
            }
        };
    }
}
