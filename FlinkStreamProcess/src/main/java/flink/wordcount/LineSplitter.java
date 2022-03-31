package flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        Stream.of(value.toLowerCase().split("\\W+"))
                .filter(t -> t.length() > 0)
                .forEach(token -> out.collect(new Tuple2<>(token, 1)));
    }
}

