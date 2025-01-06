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
package net.utp4j.channels.impl.conn;

import net.utp4j.channels.impl.UTPClient;
import net.utp4j.data.UtpPacket;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Class that resends the syn packet a few times.
 *
 * @author Ivan Iljkic (i.iljkic@gmail.com)
 */
public class ConnectionTimeOutRunnable implements Runnable {

    private final UtpPacket synPacket;
    private final UTPClient channel;
    private final ReentrantLock lock;

    public ConnectionTimeOutRunnable(UtpPacket packet,
                                     UTPClient channel, ReentrantLock lock) {
        this.synPacket = packet;
        this.lock = lock;
        this.channel = channel;
    }

    @Override
    public void run() {
        channel.resendSynPacket(synPacket);
    }


}
