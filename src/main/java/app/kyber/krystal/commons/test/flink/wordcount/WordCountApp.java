package app.kyber.krystal.commons.test.flink.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // given
        List<String> lines = Arrays.asList("This is a first sentence", "This is a second sentence with a one word");

        // when
        DataSet<Tuple2<String, Integer>> result = WordCount.startWordCount(env, lines);

        // then

//        result.print();
        result.writeAsCsv("result.txt","\n", " ");
        env.execute();
    }
}
