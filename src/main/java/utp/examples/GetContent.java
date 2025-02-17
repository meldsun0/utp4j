package utp.examples;

import utp.network.TransportLayer;
import utp.network.udp.UDPTransportLayer;
import utp.UTPClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class GetContent {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        UDPTransportLayer udpTransportLayer = new UDPTransportLayer("localhost", 13345);

        UTPClient chanel = new UTPClient(udpTransportLayer);
        startListeningIncomingPackets(udpTransportLayer, chanel);

        chanel.connect(333)
                .thenCompose(v -> chanel.read())
                .thenApply((data) -> {
                    System.out.println(StandardCharsets.UTF_8.decode(data));
                    saveAnswerOnFile(data, "content");
                    return CompletableFuture.completedFuture(data);
                }).get();
    }

    public static void startListeningIncomingPackets(TransportLayer transportLayer, UTPClient utpClient) {
        CompletableFuture.runAsync(() -> {
            while (true) {
                if (utpClient.isAlive()) {
                    try {
                        utpClient.receivePacket(transportLayer.onPacketReceive());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    public static void saveAnswerOnFile(ByteBuffer byteBuffer, String name) {
        if (byteBuffer != null) {
            try {
                byteBuffer.flip();
                File outFile = new File("testData/" + name + ".data");
                FileOutputStream fileOutputStream = new FileOutputStream(outFile);
                FileChannel fchannel = fileOutputStream.getChannel();
                while (byteBuffer.hasRemaining()) {
                    fchannel.write(byteBuffer);
                }
                fchannel.close();
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
