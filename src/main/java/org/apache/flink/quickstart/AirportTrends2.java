package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.quickstart.AirportTrends2.JFKTerminal.*;


public class AirportTrends2 {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid) {
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid) {
            for (JFKTerminal terminal : values()) {
                if (terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }

        public int getValue() {
            return mapGrid;
        }
    }


    public static class JFKFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            int originGrid = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int destGrid = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            JFKTerminal termorig = JFKTerminal.gridToTerminal(originGrid);
            JFKTerminal termdest = JFKTerminal.gridToTerminal(destGrid);


            return termorig != NOT_A_TERMINAL || termdest != NOT_A_TERMINAL;
        }
    }

    public static class JFKTerminalMatcher
            implements MapFunction<TaxiRide, Tuple2<JFKTerminal, Integer>> {

        @Override
        public Tuple2<JFKTerminal, Integer> map(TaxiRide taxiRide) throws Exception {
            int originGrid = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int destGrid = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            JFKTerminal termorig = JFKTerminal.gridToTerminal(originGrid);
            JFKTerminal termdest = JFKTerminal.gridToTerminal(destGrid);
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(taxiRide.startTime.getMillis());
            int hour = calendar.get(Calendar.HOUR_OF_DAY);

            if (termorig != NOT_A_TERMINAL) {
                return new Tuple2<>(termorig, hour);
            } else if (termdest != NOT_A_TERMINAL) {
                return new Tuple2<>(termdest, hour);
            } else return null;
        }
    }


    public static class RideCounter
            implements AllWindowFunction<Tuple2<JFKTerminal, Integer>, Tuple3<JFKTerminal, Integer, Integer>, TimeWindow> {
        @SuppressWarnings("unchecked")
        @Override
        public void apply(TimeWindow timeWindow, Iterable<Tuple2<JFKTerminal, Integer>> iterable, Collector<Tuple3<JFKTerminal, Integer, Integer>> collector) throws Exception {

            LinkedHashMap<JFKTerminal, Integer> hashMap = new LinkedHashMap<>();

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(timeWindow.getStart());
            int hour = calendar.get(Calendar.HOUR_OF_DAY);

            for (Tuple2<JFKTerminal, Integer> it : iterable) {
                if (hashMap.containsKey(it.f0)) {
                    hashMap.put(it.f0, hashMap.get(it.f0) + 1);
                } else {
                    hashMap.put(it.f0, 1);
                }
            }

            for (JFKTerminal name : hashMap.keySet()) {

                String key = name.toString();
                String value = hashMap.get(name).toString();
                System.out.println(key + " " + value);
            }

            Map<JFKTerminal, Integer> sortedByValue = hashMap
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            Map.Entry<JFKTerminal, Integer> top = sortedByValue.entrySet().iterator().next();

            collector.collect(new Tuple3<>(top.getKey(), top.getValue(), hour));
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("data/nycTaxiRides.gz", 60, 2000));

        // find popular places
        DataStream<Tuple3<JFKTerminal, Integer, Integer>> riding = rides
                // remove all rides which are not within JFK
                .filter(new JFKFilter())
                // match ride to terminal
                .map(new JFKTerminalMatcher())
                // build sliding window
                .timeWindowAll(Time.minutes(60))
                // count ride events in window
                .apply(new RideCounter());
        // print the filtered stream
        riding.print();
        env.execute();

    }
}