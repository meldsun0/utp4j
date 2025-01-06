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

import net.utp4j.channels.futures.UtpConnectFuture;
import net.utp4j.channels.futures.UtpReadFuture;
import net.utp4j.channels.impl.UTPClient;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class UTPClientSendData {
    public static void main(String[] args) throws IOException, InterruptedException {
        ByteBuffer RB = ByteBuffer.allocate(150000000);
        while (true) {
            UTPClient chanel = UTPClient.open();
            UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("localhost", 13344));
            cFuture.block();

            if(cFuture.isSuccessfull()){
                System.out.println("Starting reading");
                UtpReadFuture a = chanel.read(RB);
                a.setListener(new SaveFileListener("answer"));
                a.block();
                while(!a.isDone()){
                    System.out.println("reading");
                }
                chanel.close();
                chanel = null;

            }else{
                System.out.println("error");
            }
        }



//        UtpWriteFuture fut = chanel.write(getDataToSend());
//        fut.block();
//        System.out.println("writing test done");









    }

    public static ByteBuffer getDataToSend() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        RandomAccessFile file = new RandomAccessFile("testData/sc S01E01.avi", "rw");
        FileChannel fileChannel = file.getChannel();
        int bytesRead;
        System.out.println("start reading from file");
        do {
            bytesRead = fileChannel.read(buffer);
        } while (bytesRead != -1);
        System.out.println("file read");
        return buffer;
    }

}
