package net.utp4j.channels.impl.message;

public interface MessageHandler<Message> {

    void handle(Message message);
}
