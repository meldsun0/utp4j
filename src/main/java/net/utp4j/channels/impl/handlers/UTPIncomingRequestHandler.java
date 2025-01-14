package net.utp4j.channels.impl.handlers;

import net.utp4j.channels.impl.Session;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.data.UtpPacket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class UTPIncomingRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UTPIncomingRequestHandler.class);

    private final Map<MessageType, UTPMessageHandler> messageHandlers = new HashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);

    public UTPIncomingRequestHandler build(Session Session) {
        started.set(true);
        return this;
    }

    public UTPIncomingRequestHandler addHandler(MessageType messageType, UTPMessageHandler handler) {
        if (started.get()) {
            throw new RuntimeException("IncomingRequestProcessor already started, couldn't add any handlers");
        }
        this.messageHandlers.put(messageType, handler);
        return this;
    }


    public void messageReceive(Session session, DatagramPacket udpPacket) {
        UtpPacket message = UTPWireMessageDecoder.decode(udpPacket);
        UTPMessageHandler handler = messageHandlers.get(message.getMessageType());
        if (handler == null) {
            LOG.info("{} message not expected in UTP", message.getMessageType());
        }
        handler.handle(session, udpPacket);
    }
}
