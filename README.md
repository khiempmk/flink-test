# Flink use-case test
## 1. Install flink on docker
    Step 1: Install docker : https://docs.docker.com/desktop/mac/install/
    Step 2: Install docker-compose (MacOS- not need )
    Step 3: Start flink by start docker-compose (pls check 'docker-compose/docker-compose.yml' for components detail ):
        %cd docker-compose
        %docker-compose up -d

    Note: when flink on successful boot: tcp port 6123 is open for job manager(submit, kill, ..), web-ui available on port 8081  
## 2. Dataset API (For batch processing) - Simple Word Count use-case
Very first user-case, the word count problem is one that is commonly used to showcase the capabilities of Big Data processing frameworks. The basic solution involves counting word occurrences in a text input
#### Init environment:
- The entry point to the Flink program is an instance of the ExecutionEnvironment class — this defines the context in which a program is executed.
- Create an ExecutionEnvironment to start processing
 

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

#### Create dataset:

- Dateset can be created from multiple source (kafka, file, ...)
- For demo purpose, create on-memory dataset:


    List<String> lines = Arrays.asList("This is a first sentence", "This is a second sentence with a one word");
        
#### Process by Flink:

- Tokenize sentences into words, group by key and apply SUM - Aggregations


    public static DataSet<Tuple2<String, Integer>> startWordCount(ExecutionEnvironment env, List<String> lines) throws Exception {
        
        DataSet<String> text = env.fromCollection(lines);

        return text.flatMap(new LineSplitter()).groupBy(0).aggregate(Aggregations.SUM, 1);

    }
    
#### Consume result:

- Result store to local in CSV format by command writeAsCsv()
- Note that result can send to multiple destinations (such as HDFS, Message Queues, File, ...)

#### Submit job to cluster
    Step 1: package code into jar:

        $ mvn clean install -DskipTest=True

    Step 2: submit to flink cluster:

        Submit via CLI: 

            $ <flink-directory>/bin/flink run -c app.kyber.krystal.commons.test.flink.wordcount.WordCountApp target/flink_test-1.0-SNAPSHOT-jar-with-dependencies.jar

        Or submit via web UI (port 8081)

    Note: Use pre-packaged jar instead ('target/flink_test-1.0-SNAPSHOT-jar-with-dependencies.jar') in order to skip step 1
## 3. DataStream API ( For stream processing) - Twitter stream use-case

#### Init environment:
- For stream processing, the entry point now is 'StreamExecutionEnvironment' instead of 'ExecutionEnvironment'


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

#### Datasource:

- For demo purpose, used the flink-twitter-connector to pull realtime tweets from Twitter.

  
    DataStream<String> stream = env.addSource(TwitterSourceCreator.create())
#### Process by Flink:

- Do some simple map and filter function on data stream
- Result cleaned data

        DataStream<String> stream = env.addSource(TwitterSourceCreator.create())
                .filter(new FilterByNewTweets());

        DataStream<StreamedTweet> enriched =
                AsyncDataStream.unorderedWait(stream, new EnrichTweet(), 5000, TimeUnit.MILLISECONDS)
                        .map(new ConvertJsonIntoEnrichedTweet())
                        // Ignore rate limit errors
                        .filter(Objects::nonNull)
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

#### Consume result:

#### Submit job to cluster

    

    same as above, main class: app.kyber.krystal.commons.test.flink.twitter.TwitterStreamApp


##References:

- https://lankydan.dev/processing-tweets-with-apache-flink-and-the-twitter-api
- https://www.baeldung.com/apache-flink
    