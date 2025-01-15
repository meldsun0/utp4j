package utp.examples;


import utp.UTPServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestRead {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        UTPServer server = new  UTPServer(new InetSocketAddress(13345));
        server.start();


        CompletableFuture<Void> readFuture = server.read(buffer);
        readFuture.get();
        saveAnswerOnFile(buffer, "hi");
        server.close();
    }


    public static void saveAnswerOnFile(ByteBuffer byteBuffer, String name) {
        if (byteBuffer != null) {
            try {
                byteBuffer.flip();
                File outFile = new File("testData/"+name+" .avi");
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