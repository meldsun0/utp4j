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
package net.utp4j.examples.configtest;

import net.utp4j.channels.futures.UtpConnectFuture;
import net.utp4j.channels.futures.UtpWriteFuture;
import net.utp4j.channels.impl.UTPClient;
import net.utp4j.data.MicroSecondsTimeStamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConfigTestWrite {
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private static FileChannel fChannel;
    private static RandomAccessFile aFile;
    private static final long CPU_LOAD_CHECK_INTERVALL_MILLIS = 50;
    private static final NumberFormat percentFormat = NumberFormat.getPercentInstance(Locale.US);

    private static final Logger log = LoggerFactory.getLogger(ConfigTestWrite.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        String testPlan = "testPlan/testplan2.csv";
        String testDataFile = "testData/sc S01E01.avi";
        boolean waitOnManualInput = true;


        MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();

        CpuLoadMeasure cpuLoad = new CpuLoadMeasure();
        executor.scheduleWithFixedDelay(cpuLoad, CPU_LOAD_CHECK_INTERVALL_MILLIS, CPU_LOAD_CHECK_INTERVALL_MILLIS, TimeUnit.MILLISECONDS);

        openLog();

        ConfigTestPlanReader plan = new ConfigTestPlanReader(testPlan);
        plan.read();

        ByteBuffer buffer = ByteBuffer.allocate(150000000);
        while (plan.hasNext()) {
            String testRunLogEntry = plan.next();
            RandomAccessFile file = new RandomAccessFile(testDataFile, "rw");
            FileChannel fileChannel = file.getChannel();
            int bytesRead;
            log.debug("start reading from file");
            do {
                bytesRead = fileChannel.read(buffer);
            } while (bytesRead != -1);
            log.debug("file read");

            UTPClient chanel = UTPClient.open();
            int bytesToSend = buffer.position();

            if (waitOnManualInput) {
                System.out.println("Press any key to continue...");
                System.in.read();
            }

//			UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("192.168.1.40", 13344));
            UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("localhost", 13344));
//			UtpConnectFuture cFuture = chanel.connect(new InetSocketAddress("192.168.1.44", 13344));

            cFuture.block();
            if (cFuture.isSuccessfull()) {
                long start = timeStamper.timeStamp();
                UtpWriteFuture writeFuture = chanel.write(buffer);
                writeFuture.block();
                if (!writeFuture.isSuccessfull()) {
                    plan.failed();
                    log.debug("FAILED");
                } else {
                    String logEntry = testRunLogEntry + " -- " + calculateRate(bytesToSend, start, timeStamper.timeStamp());
                    logEntry += getCpuLoad(cpuLoad);
                    log.debug(logEntry);
                    writeEntry(logEntry + "\n");
                }
                log.debug("writing test done");
            } else {
                log.debug("FAILED");
                plan.failed();
            }
            file.close();
            fileChannel.close();
            chanel.close();
            buffer.clear();
            cpuLoad.reset();
            Thread.sleep(15000);

        }
        closeLog();
        executor.shutdown();
    }

    /* CPU */
    private static String getCpuLoad(CpuLoadMeasure cpuLoad) {
        double cpu = cpuLoad.getAverageCpu();
        percentFormat.setMaximumFractionDigits(5);
        return " -- " + percentFormat.format(cpu);
    }

    /* LOGGING methods	 */
    private static void closeLog() throws IOException {
        if (aFile != null) {
            aFile.close();
        }
        if (fChannel != null) {
            fChannel.close();
        }
    }

    private static void writeEntry(String entry) {
        ByteBuffer bbuffer = ByteBuffer.allocate(entry.getBytes().length + 10);
        bbuffer.put(entry.getBytes());

        bbuffer.flip();
        while (bbuffer.hasRemaining()) {
            try {
                fChannel.write(bbuffer);
            } catch (IOException e) {
                System.err.println("COULD NOT WRITE: " + entry);
                e.printStackTrace();
            }
        }
    }

    private static void openLog() throws IOException {
        aFile = new RandomAccessFile("testData/auto/AutoTestLog.txt", "rw");
        fChannel = aFile.getChannel();
        fChannel.truncate(0);
    }

    /* Transmission Rate calculus */
    private static String calculateRate(int bytesToSend, long start, long end) {
        long seconds = (end - start) / 1000000;
        long sendRate = 0;
        if (seconds != 0) {
            sendRate = (bytesToSend / 1024) / seconds;
        }
        return sendRate + "kB/sec";
    }


}
