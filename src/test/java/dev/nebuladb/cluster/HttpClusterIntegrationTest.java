package dev.nebuladb.cluster;

import dev.nebuladb.net.Transports;

class HttpClusterIntegrationTest extends ClusterIntegrationTestBase {
    @Override
    Transports.Kind transport() {
        return Transports.Kind.HTTP;
    }
}
