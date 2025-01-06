package net.utp4j.examples;


import net.utp4j.channels.futures.UtpAcceptFuture;
import net.utp4j.channels.futures.UtpReadFuture;
import net.utp4j.channels.impl.UTPServer;
import net.utp4j.channels.impl.UtpSocketChannelImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class TestRead {

    public static void main(String[] args) throws IOException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        UTPServer server = new  UTPServer(new InetSocketAddress(13344));
        UtpAcceptFuture acceptFuture = server.start();
        acceptFuture.block();
        UtpSocketChannelImpl channel = acceptFuture.getChannel();
        UtpReadFuture readFuture = channel.read(buffer);
        readFuture.setListener(new SaveFileListener("hi"));
        readFuture.block();
        System.out.println("reading end");
        channel.close();
        server.close();


    }


}