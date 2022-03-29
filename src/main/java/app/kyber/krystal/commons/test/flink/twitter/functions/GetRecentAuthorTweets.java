package app.kyber.krystal.commons.test.flink.twitter.functions;

import app.kyber.krystal.commons.test.flink.twitter.client.TwitterClient;
import app.kyber.krystal.commons.test.flink.twitter.data.StreamedTweet;
import app.kyber.krystal.commons.test.flink.twitter.json.EnrichedTweet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;

public class GetRecentAuthorTweets extends RichAsyncFunction<StreamedTweet, Tuple2<StreamedTweet, EnrichedTweet>> {

    private transient ObjectMapper mapper;
    private transient TwitterClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        client = TwitterClient.create();
    }

    @Override
    public void asyncInvoke(StreamedTweet input, ResultFuture<Tuple2<StreamedTweet, EnrichedTweet>> resultFuture) {
        client.getRecentTweetsByAuthor(input.getTweet().getAuthorId())
            .thenAccept(result -> {
                try {
                    EnrichedTweet tweets = mapper.readValue(result, EnrichedTweet.class);
                    resultFuture.complete(Arrays.asList(Tuple2.of(input, tweets)));
                } catch (JsonProcessingException e) {
                    resultFuture.completeExceptionally(e);
                }
            });
    }
}
