package com.sample.message.bus.poc.nats.jetstream.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.nativex.hint.TypeHint;



@ComponentScan(basePackages = "com.sample.message.bus.poc.nats.jetstream.client.controller")
/* include the Spring Native @TypeHint to enable NATS SocketDataPort inclusion 
in executable image*/
@TypeHint(types = io.nats.client.impl.SocketDataPort.class, typeNames = "io.nats.client.impl.SocketDataPort")
@SpringBootApplication
public class SampleClientServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SampleClientServiceApplication.class, args);
	}

	// subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {

}
