package app.kyber.krystal.commons.twitter;

import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

@Slf4j
public class StreamTweetProcessor {
    private String bootstrap_servers = ConfigurationLoader.getInstance().getAsString("bootstrap.servers","localhost:9092;localhost:9094");
 //   private String group_id = vn.hust.kstn.ConfigurationLoader.getInstance().getAsString("group_id", "abc");
    private String topic = ConfigurationLoader.getInstance().getAsString("topic", "twittertweet");
    private Long sleepTime = ConfigurationLoader.getInstance().getAsLong("sleep.time",10);
    private boolean isSleep = ConfigurationLoader.getInstance().getAsBoolean("is.sleep",true);
    public static void main(String[] args) {
        StreamTweetProcessor processor = new StreamTweetProcessor();
        processor.streamStart();
    }
    public void streamStart() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrap_servers);
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        Producer<String,String> producer = new KafkaProducer<String, String>(prop);
        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                try {
                    log.info("Streaming id {} " , status.getId());
                    producer.send(new ProducerRecord<>(topic, new ObjectMapper().writeValueAsString(status)));
                    if (isSleep)
                    Thread.sleep(sleepTime);
                } catch (JsonProcessingException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false)
                .setOAuthConsumerKey("I1fjAomYiRE6Y5HG6jcftdVoL")
                .setOAuthConsumerSecret("cL1FNqXrcTfK5fXIBA9HjctCPJv1LAQDp2rvg9aO4rDFhVxtuW")
                .setOAuthAccessToken("1230868141301026816-EDm43HgdgMLkEVtojHD0Tj36BYOGjv")
                .setOAuthAccessTokenSecret("LqSjY7zEuYyN2j9w4axwjGq2KE7VDrtAelUvxoREqe1Ml");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        twitterStream.sample();
      //  producer.close();
    }

}