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
import scala.Int;

import java.util.Calendar;
import java.util.TimeZone;

import static org.apache.flink.quickstart.AirportTrends.JFKTerminal.*;


public class AirportTrends {

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

    public static class JFKTerminalMatcher implements MapFunction<TaxiRide, Tuple2<JFKTerminal, Integer>> {

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

            if(termorig != NOT_A_TERMINAL){
                return new Tuple2<>(termorig, hour);
            } else if(termdest != NOT_A_TERMINAL){
                return new Tuple2<>(termdest, hour);
            }
            else return null;
        }
    }



    public static class RideCounter implements WindowFunction<Tuple2<JFKTerminal, Integer>, Tuple3<JFKTerminal, Integer, Integer>, Tuple, TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<JFKTerminal, Integer>> values,
                Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {

            JFKTerminal terminal = ((Tuple2<JFKTerminal, Integer>)key).f0;
            Integer hour = ((Tuple2<JFKTerminal, Integer>)key).f1;

            int cnt = 0;
            for(Tuple2<JFKTerminal, Integer> v : values) {
                cnt += 1;
            }

            out.collect(new Tuple3<>(terminal, cnt, hour));
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
                // partition by cell id and event type
                .<KeyedStream<JFKTerminal, Integer>>keyBy(0, 1)
                // build sliding window
                .timeWindow(Time.minutes(60))
                // count ride events in window
                .apply(new RideCounter())
                .timeWindowAll(Time.minutes(60))
                .trigger(EventTimeTrigger.create())
                .maxBy(1)
                .map(new MapFunction<Tuple3<JFKTerminal, Integer, Integer>, Tuple3<JFKTerminal, Integer, Integer>>() {
                    @Override
                    public Tuple3<JFKTerminal, Integer, Integer> map(Tuple3<JFKTerminal, Integer, Integer> input) throws Exception {
                        return new Tuple3<>(input.f0, input.f1, input.f2);
                    }
                });


        // print the filtered stream
        riding.print();

        env.execute();

    }
}