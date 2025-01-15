package utp;

import org.apache.tuweni.bytes.Bytes;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class UTPManager {

    // IP address + port + Discovery v5 NodeId + connection_id
    private final Map<Integer, UTPClient> connections = new HashMap<>();



    public CompletableFuture<ByteBuffer> getContent(int connectionId, Bytes nodeRecord, InetSocketAddress socketAddress) {
        UTPClient utpClient = this.registerClient(connectionId, socketAddress);
        return utpClient.connect(connectionId)
                .thenCompose(result -> {
                    ByteBuffer buffer = ByteBuffer.allocate(150000000);
                    return utpClient.read(buffer).thenApply(readResult -> buffer);
                })
                .exceptionally(error -> {
                    throw new RuntimeException("Operation failed", error);
                });
    }


    private UTPClient registerClient(int connectionId, InetSocketAddress socketAddress){
        UTPClient utpClient = new UTPClient(new UDPTransportLayer(socketAddress));
        if(!connections.containsKey(connectionId)) {
            this.connections.put(connectionId & 0xFFFF,utpClient);
        }
        return utpClient;
        //TODO close if present
    }

    private void removeClient(int connectionId){
        connections.remove((int) connectionId & 0xFFFF);
    }

}
