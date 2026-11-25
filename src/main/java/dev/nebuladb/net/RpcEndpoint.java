package dev.nebuladb.net;

import java.io.IOException;

/** Server side of a node's RPCs. */
public interface RpcEndpoint {
    void start() throws IOException;

    void stop();
}
