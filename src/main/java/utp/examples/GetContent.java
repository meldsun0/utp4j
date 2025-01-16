package utp.examples;

import utp.data.UtpPacket;
import utp.network.TransportLayer;
import utp.network.UDPTransportLayer;
import utp.UTPClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class GetContent {


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        UDPTransportLayer udpTransportLayer = new UDPTransportLayer("localhost", 13345);

        UTPClient chanel = new UTPClient(udpTransportLayer);
        startListeningIncomingPackets(udpTransportLayer, chanel);

        CompletableFuture<Void> cFuture = chanel.connect(333);
        cFuture.get();


        ByteBuffer buffer = ByteBuffer.allocate(10);
        CompletableFuture<Void> fut = chanel.read(buffer);
        fut.get();
        System.out.println("writing test done");
        chanel.stop();

        saveAnswerOnFile(buffer, "getContent");


    }

    public static void startListeningIncomingPackets(TransportLayer transportLayer, UTPClient utpClient) {
        CompletableFuture.runAsync(() -> {
            while(true){
            if(utpClient.isAlive()) {
                try {
                    utpClient.receivePacket(transportLayer.onPacketReceive());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }}
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
