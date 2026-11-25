package dev.nebuladb.cluster;

import dev.nebuladb.net.Transports;

class GrpcClusterIntegrationTest extends ClusterIntegrationTestBase {
    @Override
    Transports.Kind transport() {
        return Transports.Kind.GRPC;
    }
}
