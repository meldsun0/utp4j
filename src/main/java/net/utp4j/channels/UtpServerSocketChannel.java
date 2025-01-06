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

package net.utp4j.channels;

import net.utp4j.channels.futures.UtpAcceptFuture;
import net.utp4j.channels.impl.UTPServer;
import net.utp4j.channels.impl.recieve.UtpRecieveRunnable;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;


public abstract class UtpServerSocketChannel {

    /*underlying socket*/
    protected DatagramSocket socket;



    protected DatagramSocket getSocket() {
        return socket;
    }


    /**
     * Closes this server and unbinds the underlying UDP Socket.
     * The server cannot be closed while there are still open Channels.
     */
    public abstract void close();

}
