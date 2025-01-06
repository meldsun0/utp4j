/* Copyright 2013 Ivan Iljkic
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.utp4j.examples;


import net.utp4j.channels.futures.UtpAcceptFuture;
import net.utp4j.channels.futures.UtpReadFuture;
import net.utp4j.channels.futures.UtpWriteFuture;
import net.utp4j.channels.impl.UTPServer;
import net.utp4j.channels.impl.UtpSocketChannelImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class UTPServerReadData {

    public static void main(String[] args) throws IOException, InterruptedException {

        UTPServer server = new UTPServer(new InetSocketAddress(13344));


        UtpAcceptFuture acceptFuture = server.start();
        acceptFuture.block();


        System.out.println("Send data");
        UtpSocketChannelImpl channel = acceptFuture.getChannel();
        UtpWriteFuture writeFuture = channel.write( getDataToSend());
        writeFuture.block();
        if (writeFuture.isSuccessfull()) {
            System.out.println("data send ok");
        }else{
            System.out.println("data send error");
        }
        channel.close();
        server.close();
    }

    public static void readData(UtpSocketChannelImpl channel) throws InterruptedException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        UtpReadFuture readFuture = channel.read(buffer);
        readFuture.setListener(new SaveFileListener(null));
        readFuture.block();
        System.out.println("reading end");
    }

    public static ByteBuffer getDataToSend() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        RandomAccessFile file = new RandomAccessFile("testData/sc S01E01.avi", "rw");
        FileChannel fileChannel = file.getChannel();
        int bytesRead;
        do {
            bytesRead = fileChannel.read(buffer);
        } while (bytesRead != -1);
        return buffer;
    }
}
