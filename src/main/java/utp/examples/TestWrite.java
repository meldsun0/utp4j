package utp.examples;

import utp.UTPClient;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestWrite {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        RandomAccessFile file = new RandomAccessFile("testData/sc S01E01.avi", "rw");
        FileChannel fileChannel = file.getChannel();
        int bytesRead;
        System.out.println("start reading from file");
        do {
            bytesRead = fileChannel.read(buffer);
        } while (bytesRead != -1);
        System.out.println("file read");

        UTPClient chanel = new UTPClient();
        CompletableFuture<Void> cFuture = chanel.connect(new InetSocketAddress("localhost", 13345), 333);
        cFuture.get();

        CompletableFuture<Void> fut = chanel.write(buffer);
        fut.get();
        System.out.println("writing test done");
        chanel.stop();

    }

}