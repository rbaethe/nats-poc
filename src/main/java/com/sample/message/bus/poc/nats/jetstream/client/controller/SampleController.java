package com.sample.message.bus.poc.nats.jetstream.client.controller;

import io.nats.client.*;
import io.nats.client.api.*;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class provides a REST endpoint to test our <b>NATS JetStream</b>
 * integration/
 *
 * @author cwoodward
 */
@RestController
@Slf4j
public class SampleController implements ConnectionListener {

    @Value("${nats.servers}")
    private String[] servers;

    private Connection connection;
    private JetStreamManagement jetStreamManagement;

    private static final String STREAM_NAME = "ORDERS";
    private static final String CONSUMER_1 = "EXPORTER";
    private static final String CONSUMER_RATE_LIMITED = "EXPORTER_RATE_LIMITED";

    private static final String CONSUMER_SUBJECT_1 = STREAM_NAME + "." + CONSUMER_1;


    private static final String DEFAULT_MESSAGE = "a new order message";


    /**
     * Get the stream information
     */
    @GetMapping(value = "/stream-info/{streamName}")
    public StreamInfo getStreamInfo(@PathVariable String streamName) throws IOException, JetStreamApiException {
        return jetStreamManagement.getStreamInfo(streamName);

    }

    @GetMapping(value = "/consumer-infos/{streamName}")
    public List<ConsumerInfo> getConsumerInfo(@PathVariable String streamName) throws IOException, JetStreamApiException {
        return jetStreamManagement.getConsumers(streamName);
    }

    /**
     * Get the message information
     */
    @GetMapping(value = "/messages/{streamName}/{sequenceId}")
    public MessageInfo getMessage(@PathVariable String streamName, @PathVariable long sequenceId) throws IOException, JetStreamApiException {
        return jetStreamManagement.getMessage(streamName, sequenceId);
    }


    @GetMapping(value = "/pub/{message}")
    public StreamInfo publishMessage(@PathVariable String message, @RequestHeader("message-id") String messageId) throws IOException {

        /* if the user doesn't supply a message, use the default message */
        if (message == null) {
            message = DEFAULT_MESSAGE;
        }

        /* get a JetStream instance from the current nats connection*/
        JetStream js = connection.jetStream();

        try {
            /* publish the message to the stream */
            // PublishAck pubAck = js.publish(CONSUMER_SUBJECT_1, message.getBytes());

            PublishOptions opts = PublishOptions.builder().expectedStream(STREAM_NAME).messageId(messageId).build();
            PublishAck pubAck = js.publish(CONSUMER_SUBJECT_1, message.getBytes(), opts);

            return getStreamInfo(STREAM_NAME);

        } catch (JetStreamApiException ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;
    }
    /**
     * Publishes number of messages
     *
     * @param messageCount
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/load/{messageCount}")
    public void publishMessage(@PathVariable int messageCount) throws IOException {

        preLoadStreamMessages(messageCount);
    }



    private void handleMessage(Message msg) {

        Date date = new Date();

        System.out.println("\nMessage Received ( " + new Timestamp(date.getTime()) + ":");
        if (msg.hasHeaders()) {
            System.out.println("  Headers:");
            for (String key: msg.getHeaders().keySet()) {
                for (String value : msg.getHeaders().get(key)) {
                    System.out.printf("    %s: %s\n", key, value);
                }
            }
        }

        //System.out.printf("  Subject: %s\n  Data: %s\n", msg.getSubject(), new String(msg.getData(), UTF_8));
        System.out.printf("  Subject: %s\n ", msg.getSubject());
        System.out.println("  " + msg.metaData());
        System.out.println("Message size: " + msg.getData().length + " bytes, " + msg.getData().length * 8 + " Bit");

        RestTemplate restTemplate = new RestTemplate();
        String requestUrl
                = "http://localhost:7077/anything";
        //requestUrl = fooResourceUrl + new String(msg.getData());

        try {

            //ResponseEntity<String> response = restTemplate.getForEntity(requestUrl, String.class);

            ResponseEntity<String> response = restTemplate.postForEntity(requestUrl,msg.getData(), String.class );

            if ( response.getStatusCode().equals(HttpStatus.OK)) {
                msg.ack();
                System.out.println("Message sent to downstream");
                System.out.println("\n");
            }

        }catch (Exception e){
            System.out.println("Message failed to sent to downstream: " + e.getMessage());
            System.out.println("requestUrl: " + requestUrl);
            System.out.println("\n");
            throw e;
        }


    }


    public void consumerPushRateLimited() {

        log.info("consumerPush - RateLimited");

        MessageHandler handler = (Message msg) -> {
            // see handleMessage in above example
            handleMessage(msg);
        };


        try {
            /* get a JetStream instance */
            JetStream js = connection.jetStream();

            /* configure a Consumer */
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(CONSUMER_RATE_LIMITED)
                    .deliverSubject(CONSUMER_SUBJECT_1)
                    .maxDeliver(3)
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(10000)
                    //.maxAckPending(3)
                    .sampleFrequency("100")
                    .rateLimit(5)
                    .build();

            PushSubscribeOptions so = PushSubscribeOptions.builder()
                    .stream(STREAM_NAME)
                    .configuration(consumerConfig)
                    .build();

            System.out.println(so.getConsumerConfiguration().toJson());

            Dispatcher dispatcher = connection.createDispatcher();


            //JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, STREAM_NAME, dispatcher, handler, true, so);

            JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, dispatcher, handler, false, so);
            /* create the subscription */
            // JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, so);
            connection.flush(Duration.ofSeconds(5));


            connection.flush(Duration.ofSeconds(5));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (TimeoutException timeoutException) {
            timeoutException.printStackTrace();
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        } catch (JetStreamApiException jetStreamApiException) {
            jetStreamApiException.printStackTrace();
        }


    }






    /**
     * Create a connection to the configured NATS servers.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private Connection getConnection() {
        if ((connection == null) || (connection.getStatus() == Connection.Status.DISCONNECTED)) {

            Options.Builder connectionBuilder = new Options.Builder().connectionListener(this);

            /* iterate over the array of servers and add them to the  connection builder.
             */
            for (String server : servers) {
                String natsServer = "nats://" + server;
                log.info("adding nats server:" + natsServer);
                connectionBuilder.server(natsServer).maxReconnects(-1);
            }

            try {
                connection = Nats.connect(connectionBuilder.build());
            } catch (IOException | InterruptedException ex) {
                log.error(ex.getMessage());
            }
        }
        log.info("return connection:" + connection);
        return connection;
    }

    /**
     * Listen for NATS connection events.
     *
     * @param cnctn
     * @param event
     */
    @Override
    public void connectionEvent(Connection cnctn, Events event) {
        log.info("Connection Event:" + event);

        switch (event) {

            case CONNECTED:
                log.info("CONNECTED to NATS!");
                break;
            case DISCONNECTED:
                log.warn("RECONNECTED to NATS!");
                try {
                    connection = null;
                    getConnection();
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);

                }
                break;
            case RECONNECTED:
                log.info("RECONNECTED to NATS!");
                break;
            case RESUBSCRIBED:
                log.info("RESUBSCRIBED!");
                break;

        }

    }

    /**
     * perform basic setup after the controller has been created.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @PostConstruct
    void postConstruct() throws IOException, InterruptedException {
        try {
            log.info("REST controller postConstruct.");
            createStream();
            preLoadStreamMessages(10);
            consumerPushRateLimited();

        } catch (JetStreamApiException ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    /**
     * Create the stream we will be using.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws JetStreamApiException
     */
    private void createStream() throws IOException, InterruptedException, JetStreamApiException {
        log.info("creating stream");
        connection = getConnection();

        JetStream js = connection.jetStream();
        jetStreamManagement = connection.jetStreamManagement();

        /* if the stream already exists, delete it */
        StreamInfo streamInfo = null;

        try {
            streamInfo = jetStreamManagement.getStreamInfo(STREAM_NAME);
            if (streamInfo != null) {
                log.warn("Stream already exists....");
                deleteStream(STREAM_NAME);
            }
        } catch (JetStreamApiException ex) {
            log.info("Stream does not exist");
        }

        log.info("creating stream");
        /* configure the stream */
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(STREAM_NAME)
                    .storageType(StorageType.Memory)
                    .subjects(CONSUMER_SUBJECT_1)
                    .build();

            /* create the stream */
            streamInfo = jetStreamManagement.addStream(streamConfig);
            log.info("Created Stream", streamInfo);

        } catch (JetStreamApiException jsapiEx) {
            log.error(jsapiEx.getMessage());
        }
    }


    /**
     * delete the stream.
     */
    private void deleteStream(String streamName) {
        log.info("Destroying Stream-" + streamName);
        try {
            connection = getConnection();
            JetStream js = connection.jetStream();
            jetStreamManagement = connection.jetStreamManagement();
            jetStreamManagement.deleteStream(streamName);
        } catch (IOException | JetStreamApiException ex) {
            log.warn(ex.getMessage(), ex);
        }
    }


    /**
     * publish <i>n</i> message to prime the stream for the demo.
     *
     * @throws IOException
     */
    private void preLoadStreamMessages(int count) throws IOException {
        for (int idx = 0; idx < count; idx++) {
                publishMessage(String.format("pre-loaded message (%d)", idx), UUID.randomUUID().toString());
        }
    }

    /**
     * perform cleanup before the controller is destroyed.
     */
    @PreDestroy
    private void cleanup() {
        log.info("Consumer and stream cleanup");
        deleteStream(STREAM_NAME);
    }
}
