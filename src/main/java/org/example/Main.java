package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        // Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-meteorologiques-app") ;
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());

        // Construire le flux
        StreamsBuilder builder = new StreamsBuilder();

        // Lire depuis le topic "weather-data"
        KStream<String, String> ordersStream = builder.stream("weather-data");

        // Transformation 1 : Filtrer les données de température élevée
        KStream<String, String> filteredWeather = ordersStream.filter((k, v) -> {
            double temperature = Double.parseDouble(v.split(",")[1]);
            return temperature > 30;
        });

        // Transformation 2 : Convertir les températures en Fahrenheit
        KStream<String, String> weatherInFahrenheit = filteredWeather.mapValues((k, v)-> {
            String[] data = v.split(",");
            String station = data[0];
            Double temperature = Double.valueOf(data[1]);
            Double humidity = Double.valueOf(data[2]);
            double temperatureInFahrenheit = (temperature * 9/5) + 32;

            return station+","+temperatureInFahrenheit+","+humidity;
        });

        // Transformation 3 : Grouper les données par station et calculer la température moyenne et le taux d'humidité moyen pour chaque station.
        KGroupedStream<String, String> groupedWeather  = weatherInFahrenheit.groupBy((key, value)-> value.split(",")[0]);
        // Aggregate: Use a simple String aggregation format like "sumTemp,sumHumidity,count"
        KTable<String, String> aggregatedStats = groupedWeather.aggregate(
                // Initializer for each group
                () -> "0,0,0", // Initialize as "sumTemp,sumHumidity,count"

                // Aggregator function: Parse values and update sums and count
                (key, newValue, aggregate) -> {
                    String[] newValues = newValue.split(",");
                    String[] aggValues = aggregate.split(",");

                    double newTemp = Double.parseDouble(newValues[1]);
                    double newHumidity = Double.parseDouble(newValues[2]);

                    double aggTemp = Double.parseDouble(aggValues[0]);
                    double aggHumidity = Double.parseDouble(aggValues[1]);

                    long count = Long.parseLong(aggValues[2]);

                    double updatedTemp = aggTemp + newTemp;
                    double updatedHumidity = aggHumidity + newHumidity;
                    long updatedCount = count + 1;

                    return updatedTemp + "," + updatedHumidity + "," + updatedCount;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // Compute averages and transform output to a readable format
        KTable<String, String> stationAverages = aggregatedStats.mapValues(aggregate -> {
            String[] values = aggregate.split(",");
            double sumTemp = Double.parseDouble(values[0]);
            double sumHumidity = Double.parseDouble(values[1]);
            long count = Long.parseLong(values[2]);

            double avgTemp = sumTemp / count;
            double avgHumidity = sumHumidity / count;

            return "Température Moyenne = " + avgTemp + "°F, Humidité Moyenne = " + avgHumidity + "%";
        });

        // Write results to the 'station-averages' topic
        stationAverages.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // Demarrer kafka streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Arret propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        /*
            for test :
            Station1,25.3,60
            Station2,95.0,50
            Station2,98.6,40
        */
    }
}