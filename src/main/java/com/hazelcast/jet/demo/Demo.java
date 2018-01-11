package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.demo.types.WakeTurbulanceCategory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLagAndDelay;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.demo.types.WakeTurbulanceCategory.HEAVY;
import static com.hazelcast.jet.demo.util.Util.inLondon;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;

public class Demo {

    static Map<String, Double> typeToLTOCycyleC02Emission = new HashMap<String, Double>() {{
        put("B738", 2625d);
        put("A320", 2750.7d);
        put("A321", 2560d);
        put("A319", 2169.8d);
        put("B763", 5405d);
        put("E190", 1605d);
        put("B773", 7588d);
        put("A333", 5934d);
        put("B752", 4292d);
        put("A332", 7029d);
        put("B737", 2454d);
        put("DH8D", 1950d);
        put("B772", 7346d);
        put("B788", 10944d);
        put("B739", 2500d);
        put("A388", 13048d);
        put("B744", 10456d);
        put("B789", 10300d);
        put("E170", 1516d);
        put("A306", 5200d);
        put("B77L", 9076d);
        put("MD11", 8277d);
        put("E145", 1505d);
        put("B77W", 9736d);
        put("A318", 2169d);
        put("B748", 10400d);
        put("B735", 2305d);
        put("A359", 6026d);
        put("CRJ9", 2754d);
        put("CRJ2", 1560d);
        put("CRJ7", 2010d);
        put("DC10", 7460d);
    }};

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

        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(10_000, 5_000);
        AggregateOperation1<Aircraft, List<Object>, List<Object>> allOf = allOf(toList(),
                linearTrend(Aircraft::getPosTime, Aircraft::getAlt));


        Vertex insertWm = dag.newVertex("insertWm",
                insertWatermarksP(Aircraft::getPosTime, limitingLagAndDelay(60000, 5000), emitByFrame(wDefTrend)));

        // accumulation: calculate altitude trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                accumulateByFrameP(
                        Aircraft::getId,
                        Aircraft::getPosTime, TimestampKind.EVENT,
                        wDefTrend,
                        allOf));
        Vertex trendStage2 = dag.newVertex("trendStage2", combineToSlidingWindowP(wDefTrend, allOf));

        Vertex identifyPhase = dag.newVertex("identify flight phase", Processors.mapP((TimestampedEntry<Long, List> entry) -> {
                    List value = entry.getValue();
                    Aircraft aircraft = ((List<Aircraft>) value.get(0)).iterator().next();
                    Double coefficient = (Double) value.get(1);
                    if (coefficient > 0) {
                        return Tuple2.tuple2(aircraft, "TAKING_OFF");
                    } else if (coefficient < 0) {
                        return Tuple2.tuple2(aircraft, "LANDING");
                    }
                    return null;
                }
        ));


        Vertex enrichWithNoiseInfo = dag.newVertex("enrich with noise info", Processors.mapP((Tuple2<Aircraft, String> tuple2) -> {
            Aircraft aircraft = tuple2.f0();
            String phase = tuple2.f1();
            Long altitude = aircraft.getAlt();
            Integer currentDb = getPhaseNoiseLookupTable(phase, aircraft.getWtc()).tailMap(altitude.intValue()).values().iterator().next();
            return Tuple3.tuple3(aircraft, phase, currentDb);

        }));


        Vertex averageNoise = dag.newVertex("average noise", aggregateToSlidingWindowP(
                constantKey(),
                (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f0().getPosTime(),
                TimestampKind.EVENT,
                wDefTrend,
                averagingLong(
                        (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f2()
                )
                )
        );

        Vertex pollution = dag.newVertex("average pollution", Processors.mapP((Tuple2<Aircraft, String> tuple2) -> {
            Aircraft aircraft = tuple2.f0();
            String phase = tuple2.f1();
            Long altitude = aircraft.getAlt();
            Integer currentDb = getPhaseNoiseLookupTable(phase, aircraft.getWtc()).tailMap(altitude.intValue()).values().iterator().next();
            return Tuple3.tuple3(aircraft, phase, currentDb);
        }));

        Vertex loggerSink = dag.newVertex("logger", DiagnosticProcessors.writeLoggerP());
        dag.edge(from(source, 0).to(overLondon, 0));
        dag.edge(from(overLondon, 0).to(lessThan3000, 0));

        dag
                .edge(from(lessThan3000, 0).to(insertWm, 0))
                .edge(between(insertWm, trendStage1)
                        .partitioned(Aircraft::getId))
                .edge(between(trendStage1, trendStage2)
                        .partitioned((TimestampedEntry e) -> e.getKey()).distributed())
                .edge(from(trendStage2).to(identifyPhase))
                .edge(from(identifyPhase, 0).to(enrichWithNoiseInfo))
                .edge(from(enrichWithNoiseInfo).to(averageNoise)
                                               .allToOne())
                .edge(from(averageNoise).to(loggerSink))

                .edge(from(identifyPhase, 1).to(pollution))
                .edge(from(enrichWithNoiseInfo).to(averageNoise)
                                               .allToOne())
                .edge(from(averageNoise).to(loggerSink));


        Job job = jet.newJob(dag);
        job.join();

    }

    private static SortedMap<Integer, Integer> getPhaseNoiseLookupTable(String phase, WakeTurbulanceCategory wtc) {
        if ("TAKING_OFF".equals(phase)) {
            if (HEAVY.equals(wtc)) {
                return heavyWTCClimbingAltitudeToNoiseDb;
            } else {
                return mediumWTCClimbingAltitudeToNoiseDb;
            }
        } else if ("LANDING".equals(phase)) {
            if (HEAVY.equals(wtc)) {
                return heavyWTCDescendAltitudeToNoiseDb;
            } else {
                return mediumWTCDescendAltitudeToNoiseDb;
            }
        }
        return Collections.emptySortedMap();
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
