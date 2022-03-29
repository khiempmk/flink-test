package app.kyber.krystal.commons.test.flink.twitter.functions;

import app.kyber.krystal.commons.test.flink.twitter.data.RecentTweet;
import app.kyber.krystal.commons.test.flink.twitter.data.StreamedTweet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class FilterByHasMentions implements FilterFunction<Tuple2<StreamedTweet, List<RecentTweet>>> {

    @Override
    public boolean filter(Tuple2<StreamedTweet, List<RecentTweet>> value) {
        return value.f1.stream().anyMatch(tweet -> !tweet.getMentions().isEmpty());
    }
}

