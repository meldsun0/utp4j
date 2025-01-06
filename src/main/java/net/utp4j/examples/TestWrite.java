package net.utp4j.examples;

import net.utp4j.channels.UtpSocketChannel;
import net.utp4j.channels.futures.UtpConnectFuture;
import net.utp4j.channels.futures.UtpWriteFuture;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestWrite {
    public static void main(String[] args) throws IOException, InterruptedException {

        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        RandomAccessFile file = new RandomAccessFile("testData/sc S01E01.avi", "rw");
        FileChannel fileChannel = file.getChannel();
        int bytesRead;
        System.out.println("start reading from file");
        do {
            bytesRead = fileChannel.read(buffer);
        } while (bytesRead != -1);
        System.out.println("file read");

        UtpSocketChannel chanel = UtpSocketChannel.open();
//		UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("192.168.1.40", 13344));
        UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("localhost", 13344));
//		UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("192.168.1.44", 13344));
        cFuture.block();

        UtpWriteFuture fut = chanel.write(buffer);
        fut.block();
        System.out.println("writing test done");
        chanel.close();

    }

}