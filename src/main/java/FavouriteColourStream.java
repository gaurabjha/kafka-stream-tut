import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourStream {
    public static void main(String[] args) {

        FavouriteColourStream app = new FavouriteColourStream();
        Properties config = app.getDefaultProperties();


        //Create Stream
        KStreamBuilder builder = new KStreamBuilder();
//        1. Stream from Kafka
        KStream<String, String> textLines =  builder.stream("favourite-colour-input");
//        2. Filter bad values
        KStream<String, String> usersAndColours = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired colours (could be a data sanitization step
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));
//        6. Write to Kafka as intermediary topic

        usersAndColours.to("user-keys-and-colours");

        System.out.println(usersAndColours);




//        7. Read from Kafka as a KTable
        KTable<String,String> userAndColoursTable = builder.table("user-keys-and-colours");
//        8. GroupBy Colors
        KTable<String,Long> favouriteColoursCount = userAndColoursTable.groupBy((name, colour) -> new KeyValue<>(colour, colour))
//        9. Count to count colours occurances (KTable)
        .count("CountsByColours");
//        10.Write to Kafka as final topic
        favouriteColoursCount.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");



        KafkaStreams stream = new KafkaStreams(builder, config);
        stream.cleanUp();
        stream.start();


        System.out.println(stream);

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

    public Properties getDefaultProperties(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-scala");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


        return config;
    }

}
