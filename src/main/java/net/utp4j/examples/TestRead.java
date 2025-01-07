package net.utp4j.examples;


import net.utp4j.channels.futures.UtpReadFuture;
import net.utp4j.channels.impl.UTPServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class TestRead {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        UTPServer server = new  UTPServer(new InetSocketAddress(13345));
        server.start();


        UtpReadFuture readFuture = server.read(buffer);
        readFuture.setListener(new SaveFileListener("hi"));
        readFuture.block();
        System.out.println("reading end");
        server.close();


    }


}