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
package org.graylog2.restclient.models.alerts;

import org.graylog2.rest.models.streams.alerts.AlertListSummary;
import org.graylog2.restclient.lib.APIException;
import org.graylog2.restclient.lib.ApiClient;
import org.graylog2.restroutes.generated.routes;

import javax.inject.Inject;
import java.io.IOException;

public class StreamAlertService {
    private final ApiClient api;

    @Inject
    public StreamAlertService(ApiClient api) {
        this.api = api;
    }

    public AlertListSummary listPaginated(String streamId, int skip, int limit) throws APIException, IOException {
        return api.path(routes.StreamAlertResource().listPaginated(streamId), AlertListSummary.class)
                .queryParam("skip", skip)
                .queryParam("limit", limit)
                .execute();
    }
}
