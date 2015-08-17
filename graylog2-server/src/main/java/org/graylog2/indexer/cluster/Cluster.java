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
package org.graylog2.indexer.cluster;

import com.github.joschi.jadconfig.util.Duration;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.graylog2.indexer.Deflector;
import org.graylog2.indexer.esplugin.ClusterStateMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    private final Client c;
    private final Deflector deflector;
    private final ScheduledExecutorService scheduler;
    private final Duration requestTimeout;
    private final AtomicReference<Map<String, DiscoveryNode>> nodes = new AtomicReference<>();

    @Inject
    public Cluster(Client client,
                   Deflector deflector,
                   @Named("daemonScheduler") ScheduledExecutorService scheduler,
                   @Named("elasticsearch_request_timeout") Duration requestTimeout) {
        this.scheduler = scheduler;
        this.c = client;
        this.deflector = deflector;
        this.requestTimeout = requestTimeout;
        // unfortunately we can't use guice here, because elasticsearch and graylog2 use different injectors and we can't
        // get to the instance to bridge.
        ClusterStateMonitor.setCluster(this);
    }

    public ClusterHealthResponse health() {
        ClusterHealthRequest request = new ClusterHealthRequest(deflector.getDeflectorWildcard());
        return c.admin().cluster().health(request).actionGet();
    }

    public int getNumberOfNodes() {
        return c.admin().cluster().nodesInfo(new NodesInfoRequest().all()).actionGet().getNodes().length;
    }

    public List<NodeInfo> getDataNodes() {
        List<NodeInfo> dataNodes = Lists.newArrayList();

        for (NodeInfo nodeInfo : getAllNodes()) {
            /*
             * We are setting node.data to false for our graylog2-server nodes.
             * If it's not set or not false it is a data storing node.
             */
            String isData = nodeInfo.getSettings().get("node.data");
            if (isData != null && isData.equals("false")) {
                continue;
            }

            dataNodes.add(nodeInfo);
        }

        return dataNodes;
    }

    public List<NodeInfo> getAllNodes() {
        return Lists.newArrayList(c.admin().cluster().nodesInfo(new NodesInfoRequest().all()).actionGet().getNodes());
    }

    public String nodeIdToName(String nodeId) {
        final NodeInfo nodeInfo = getNodeInfo(nodeId);
        return nodeInfo == null ? "UNKNOWN" : nodeInfo.getNode().getName();

    }

    public String nodeIdToHostName(String nodeId) {
        final NodeInfo nodeInfo = getNodeInfo(nodeId);
        return nodeInfo == null ? "UNKNOWN" : nodeInfo.getHostname();
    }

    private NodeInfo getNodeInfo(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            return null;
        }

        try {
            NodesInfoResponse r = c.admin().cluster().nodesInfo(new NodesInfoRequest(nodeId).all()).actionGet();
            return r.getNodesMap().get(nodeId);
        } catch (Exception e) {
            LOG.error("Could not read name of ES node.", e);
            return null;
        }
    }

    /**
     * Check if the Elasticsearch {@link org.elasticsearch.node.Node} is connected and that there are other nodes
     * in the cluster.
     *
     * @return {@code true} if the Elasticsearch client is up and the cluster contains other nodes, {@code false} otherwise
     */
    public boolean isConnected() {
        Map<String, DiscoveryNode> nodeMap = nodes.get();
        return nodeMap != null && !nodeMap.isEmpty();
    }

    /**
     * Check if the cluster health status is not {@link ClusterHealthStatus#RED} and that the
     * {@link org.graylog2.indexer.Deflector#isUp() deflector is up}.
     *
     * @return {@code true} if the the cluster is healthy and the deflector is up, {@code false} otherwise
     */
    public boolean isHealthy() {
        try {
            return health().getStatus() != ClusterHealthStatus.RED && deflector.isUp();
        } catch (ElasticsearchException e) {
            LOG.trace("Couldn't determine Elasticsearch health properly", e);
            return false;
        }
    }

    public void waitForConnectedAndHealthy(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        LOG.debug("Waiting until cluster connection comes back and cluster is healthy, checking once per second.");

        final CountDownLatch latch = new CountDownLatch(1);
        final ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (isConnected() && isHealthy()) {
                        LOG.debug("Cluster is healthy again, unblocking waiting threads.");
                        latch.countDown();
                    }
                } catch (Exception ignore) {} // to not cancel the schedule
            }
        }, 0, 1, TimeUnit.SECONDS); // TODO should this be configurable?

        final boolean waitSuccess = latch.await(timeout, unit);
        scheduledFuture.cancel(true); // Make sure to cancel the task to avoid task leaks!

        if(!waitSuccess) {
            throw new TimeoutException("Elasticsearch cluster didn't get healthy within timeout");
        }
    }

    public void waitForConnectedAndHealthy() throws InterruptedException, TimeoutException {
        waitForConnectedAndHealthy(requestTimeout.getQuantity(), requestTimeout.getUnit());
    }

    public void updateDataNodeList(Map<String, DiscoveryNode> nodes) {
        LOG.debug("{} data nodes in cluster", nodes.size());
        this.nodes.set(nodes);
    }
}
