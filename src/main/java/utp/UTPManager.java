package utp;

import org.apache.tuweni.bytes.Bytes;
import utp.data.UtpPacket;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class UTPManager {

    // IP address + port + Discovery v5 NodeId + connection_id
    private final Map<Integer, UTPClient> connections = new HashMap<>();



    public CompletableFuture<ByteBuffer> getContent(int connectionId, Bytes nodeRecord,  final TransportLayer transportLayer) {
        UTPClient utpClient = this.registerClient(connectionId, transportLayer);
        return utpClient.connect(connectionId)
                .thenCompose(result -> {
                    ByteBuffer buffer = ByteBuffer.allocate(150000000);
                    return utpClient.read(buffer).thenApply(readResult -> buffer);
                })
                .exceptionally(error -> {
                    throw new RuntimeException("Operation failed", error);
                });
    }


    private UTPClient registerClient(int connectionId, final TransportLayer transportLayer){
        UTPClient utpClient = new UTPClient(transportLayer);
        if(!connections.containsKey(connectionId)) {
            this.connections.put(connectionId & 0xFFFF,utpClient);
        }
        return utpClient;
        //TODO close if present
    }

    private void removeClient(int connectionId){
        connections.remove((int) connectionId & 0xFFFF);
    }

    public void onPacketReceive(DatagramPacket udp ){
        UtpPacket utpPacket = UtpPacket.decode(udp);
        UTPClient client = connections.get(utpPacket.getConnectionId() & 0xFFFF);
//        if (client != null) {
//            client.recievePacket(udp);
//        }
    }

}
