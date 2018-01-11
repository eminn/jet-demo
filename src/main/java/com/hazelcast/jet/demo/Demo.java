package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.demo.util.Util.inLondon;

public class Demo {

    static SortedMap<Integer, Integer> heavyWTCClimbingAltitudeToNoiseDb = new TreeMap<Integer, Integer>() {{
        put(500, 96);
        put(1000, 92);
        put(1500, 86);
        put(2000, 82);
        put(3000, 72);
    }};
    static SortedMap<Integer, Integer> mediumWTCClimbingAltitudeToNoiseDb = new TreeMap<Integer, Integer>() {{
        put(500, 83);
        put(1000, 81);
        put(1500, 74);
        put(2000, 68);
        put(3000, 61);
    }};
    static SortedMap<Integer, Integer> heavyWTCDescendAltitudeToNoiseDb = new TreeMap<Integer, Integer>() {{
        put(3000, 70);
        put(2000, 79);
        put(1000, 94);
        put(500, 100);
    }};
    static SortedMap<Integer, Integer> mediumWTCDescendAltitudeToNoiseDb = new TreeMap<Integer, Integer>() {{
        put(3000, 63);
        put(2000, 71);
        put(1000, 86);
        put(500, 93);
    }};

    public static void main(String[] args) {
        JetInstance jet = Jet.newJetInstance();
        String url = "https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json";

        DAG dag = new DAG();


        Vertex source = dag.newVertex("flight data source", FlightDataSource.supplier(url, 1000));
        Vertex overLondon = dag.newVertex("planes over london", Processors.filterP((Aircraft ac) -> ac.getAlt() > 0 && inLondon(ac.getLon(), ac.getLat()) && !ac.isGnd()));
        Vertex lessThan3000 = dag.newVertex("altitude less than 3000", Processors.filterP((Aircraft ac) -> ac.getAlt() > 0 && ac.getAlt() < 3000 && !ac.isGnd()));

        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(10_000, 1_000);
        AggregateOperation1<Aircraft, LinTrendAccumulator, Double> aggrOpTrend =
                AggregateOperations.linearTrend(Aircraft::getPosTime, Aircraft::getAlt);


        Vertex insertWm = dag.newVertex("insertWm",
                insertWatermarksP(Aircraft::getPosTime, withFixedLag(25000), emitByFrame(wDefTrend)));

        // accumulation: calculate altitude trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                accumulateByFrameP(
                        Aircraft::getId,
                        Aircraft::getPosTime, TimestampKind.EVENT,
                        wDefTrend,
                        aggrOpTrend));
        Vertex trendStage2 = dag.newVertex("trendStage2", combineToSlidingWindowP(wDefTrend, aggrOpTrend));

        Vertex identifyPhase = dag.newVertex("identify flight phase", Processors.mapP((TimestampedEntry<Long, Double> entry) -> {
                    if (entry.getValue() > 0) {
                        return Tuple2.tuple2(entry.getKey(), "TAKING_OFF");
                    } else if (entry.getValue() < 0) {
                        return Tuple2.tuple2(entry.getKey(), "LANDING");
                    }
                    return null;
                }
        ));


        Vertex enrichWithNoiseInfo = dag.newVertex("enrich with noise info", Processors.mapP((Aircraft ac) -> {
            Long altitude = ac.getAlt();
            Integer currentDb;
            switch (ac.getWtc()) {
                case HEAVY:
                    currentDb = heavyWTCClimbingAltitudeToNoiseDb.tailMap(altitude.intValue()).values().iterator().next();
                    return Tuple2.tuple2(ac, currentDb);
                case MEDIUM:
                case LIGHT:
                    currentDb = mediumWTCClimbingAltitudeToNoiseDb.tailMap(altitude.intValue()).values().iterator().next();
                    return Tuple2.tuple2(ac, currentDb);
                case NONE:
                    break;
            }
            return null;

        }));
        Vertex loggerSink = dag.newVertex("logger", DiagnosticProcessors.writeLoggerP());
        dag.edge(from(source, 0).to(overLondon, 0));
        dag.edge(from(overLondon, 0).to(lessThan3000, 0));
        dag.edge(from(lessThan3000, 0).to(enrichWithNoiseInfo, 0));
        dag.edge(from(enrichWithNoiseInfo, 0).to(loggerSink, 0));


        dag
                .edge(from(lessThan3000, 1).to(insertWm, 0))
                .edge(between(insertWm, trendStage1)
                        .partitioned(Aircraft::getId))
                .edge(between(trendStage1, trendStage2)
                        .partitioned((TimestampedEntry e) -> e.getKey()).distributed())
                .edge(from(trendStage2).to(identifyPhase))
                .edge(from(identifyPhase).to(loggerSink, 1)
                                       .allToOne());


        Job job = jet.newJob(dag);
        job.join();

    }


    //IMAGE ENRICHMENT
    //        ComputeStage<Tuple2<String, ?>> images = overIstanbul.map(ac -> {
//            HttpURLConnection con = null;
//            try {
//                con = (HttpURLConnection) new URL("http://www.airport-data.com/api/ac_thumb.json?m=" + ac.getIcao() + "&n=1").openConnection();
//                con.setRequestMethod("GET");
//                con.addRequestProperty("User-Agent", "Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 40.0.2214.91 Safari / 537.36");
//                con.getResponseCode();
//
//                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//                String inputLine;
//                StringBuilder response = new StringBuilder();
//                while ((inputLine = in.readLine()) != null) {
//                    response.append(inputLine);
//                }
//                in.close();
//                con.disconnect();
//
//                JsonValue value = Json.parse(response.toString());
//                JsonObject object = value.asObject();
//                if (object.get("status").asInt() == 404) {
//                    return Tuple2.tuple2(ac.getReg(), null);
//                }
//                String imageLink = object.get("data").asArray().get(0).asObject().get("image").asString();
//                return Tuple2.tuple2(ac.getReg(), imageLink);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            return Tuple2.tuple2(ac.getReg(), null);
//
//        });
//        images.drainTo(Sinks.logger());


}
