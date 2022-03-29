package app.kyber.krystal.commons.test.flink.twitter.functions;

import app.kyber.krystal.commons.test.flink.twitter.data.StreamedTweet;
import app.kyber.krystal.commons.test.flink.twitter.json.EnrichedTweet;
import app.kyber.krystal.commons.test.flink.twitter.json.EnrichedTweetData;
import app.kyber.krystal.commons.test.flink.twitter.json.Mention;
import app.kyber.krystal.commons.test.flink.twitter.json.User;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class MapToStreamedTweets implements MapFunction<EnrichedTweet, StreamedTweet> {

    @Override
    public StreamedTweet map(EnrichedTweet value) {
        EnrichedTweetData data = value.getSingleData();
        User user = getUser(value);
        if (data.getEntities() == null || data.getEntities().getMentions() == null) {
            return new StreamedTweet(data, user.getName(), user.getUsername(), new HashSet<>());
        }
        return new StreamedTweet(
            data,
            user.getName(),
            user.getUsername(),
            data.getEntities().getMentions().stream().map(Mention::getUsername).collect(Collectors.toSet())
        );
    }

    private User getUser(EnrichedTweet value) {
        return value.getIncludes()
            .getUsers()
            .stream()
            .filter(u -> u.getId().equals(value.getSingleData().getAuthorId()))
            .findFirst()
            .get();
    }
}
