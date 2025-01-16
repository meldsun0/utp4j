package utp;

import org.apache.tuweni.bytes.Bytes;
import utp.network.UDPTransportLayer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

public class Test {


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        UTPManager  utpManager= new UTPManager();
        UDPTransportLayer udpTransportLayer = new UDPTransportLayer("localhost", 13345);

        utpManager.getContent(123, Bytes.EMPTY, udpTransportLayer )
                .thenAccept(buffer -> {
                    System.out.println(buffer.array());
            saveAnswerOnFile(buffer, "test");
        }).exceptionally(error -> {
                    // Handle the error
                    error.printStackTrace();
                    return null;
        }).get();


    }

    public static void saveAnswerOnFile(ByteBuffer byteBuffer, String name) {
        if (byteBuffer != null) {
            try {
                byteBuffer.flip();
                File outFile = new File("testData/"+name+".data");
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
