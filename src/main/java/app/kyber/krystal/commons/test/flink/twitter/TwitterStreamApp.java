package app.kyber.krystal.commons.test.flink.twitter;

import app.kyber.krystal.commons.test.flink.twitter.data.Result;
import app.kyber.krystal.commons.test.flink.twitter.data.StreamedTweet;
import app.kyber.krystal.commons.test.flink.twitter.functions.*;
import app.kyber.krystal.commons.test.flink.twitter.json.EnrichedTweetData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TwitterStreamApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.addSource(TwitterSourceCreator.create())
                .filter(new FilterByNewTweets());

        DataStream<StreamedTweet> enriched =
                AsyncDataStream.unorderedWait(stream, new EnrichTweet(), 5000, TimeUnit.MILLISECONDS)
                        .map(new ConvertJsonIntoEnrichedTweet())
                        // Ignore rate limit errors
                        .filter(Objects::nonNull)
                        // some enriched tweets don't have data or authors, not sure why but filter them out anyway
                        .filter(tweet -> {
                            EnrichedTweetData data = tweet.getSingleData();
                            return data != null && data.getAuthorId() != null;
                        })
                        .map(new MapToStreamedTweets());

        DataStream<Result> results =
                AsyncDataStream.unorderedWait(enriched, new GetRecentAuthorTweets(), 5000, TimeUnit.MILLISECONDS)
                        .map(new MapToRecentTweets())
                        .map(new FilterByRepeatedMentions())
                        .filter(new FilterByHasMentions())
                        .map(new MapToResults());

        results.print();
        env.execute();
    }
}
