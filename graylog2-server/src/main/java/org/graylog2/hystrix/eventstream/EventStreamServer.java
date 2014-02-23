/**
 * Copyright 2014 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.hystrix.eventstream;

import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class EventStreamServer implements Runnable {

    @Override
    public void run() {
        Server server = new Server(8090);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        //context.setContextPath("/hystrix");

        server.setHandler(context);

        context.addServlet(new ServletHolder(new HystrixMetricsStreamServlet()), "/hystrix.stream");

        try {
            server.start();
            server.join();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
