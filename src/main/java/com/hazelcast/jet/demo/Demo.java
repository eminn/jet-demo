package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.demo.types.WakeTurbulanceCategory;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.stream.IStreamMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingTimestampAndWallClockLag;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.demo.types.WakeTurbulanceCategory.HEAVY;
import static com.hazelcast.jet.demo.util.Util.inAtlanta;
import static com.hazelcast.jet.demo.util.Util.inFrankfurt;
import static com.hazelcast.jet.demo.util.Util.inIstanbul;
import static com.hazelcast.jet.demo.util.Util.inLondon;
import static com.hazelcast.jet.demo.util.Util.inParis;

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

        Vertex emittingHelpSignal = dag.newVertex("planes that emits help signal",
                filterP(
                        (Aircraft ac) -> ac.isHelp() && !ac.isGnd()
                )
        );

        Vertex tagPlanesInInterestedCities = dag.newVertex("tag planes in interested cities",
                mapP(
                        (Aircraft ac) ->
                        {
                            // we are interested only in planes above the ground
                            if (ac.getAlt() > 0 && !ac.isGnd()) {
                                if (inLondon(ac.getLon(), ac.getLat())) {
                                    ac.setCity("London");
                                    return ac;
                                } else if (inIstanbul(ac.getLon(), ac.getLat())) {
                                    ac.setCity("Istanbul");
                                    return ac;
                                } else if (inFrankfurt(ac.getLon(), ac.getLat())) {
                                    ac.setCity("Frankfurt");
                                    return ac;
                                } else if (inAtlanta(ac.getLon(), ac.getLat())) {
                                    ac.setCity("Atlanta");
                                    return ac;
                                } else if (inParis(ac.getLon(), ac.getLat())) {
                                    ac.setCity("Paris");
                                    return ac;
                                }
                            }
                            return null;
                        }
                )
        );


        Vertex altitudeLessThan3000ft = dag.newVertex("altitude less than 3000",
                filterP(
                        (Aircraft ac) -> ac.getAlt() > 0 && ac.getAlt() < 3000 && !ac.isGnd())
        );

        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(60_000, 10_000);
        AggregateOperation1<Aircraft, List<Object>, List<Object>> allOf =
                allOf(
                        toList(),
                        linearTrend(Aircraft::getPosTime, Aircraft::getAlt)
                );

        Vertex insertWm = dag.newVertex("insertWm",
                insertWatermarksP(
                        Aircraft::getPosTime,
                        limitingTimestampAndWallClockLag(60000, 60000),
                        emitByFrame(wDefTrend))
        ).localParallelism(1);

        // accumulation: calculate altitude trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                accumulateByFrameP(
                        Aircraft::getId,
                        Aircraft::getPosTime, TimestampKind.EVENT,
                        wDefTrend,
                        allOf
                )
        );
        Vertex trendStage2 = dag.newVertex("trendStage2", combineToSlidingWindowP(wDefTrend, allOf));

        Vertex identifyPhase = dag.newVertex("identify flight phase",
                flatMapP(
                        (TimestampedEntry<Long, List> entry) -> {
                            List value = entry.getValue();
                            List<Aircraft> aircrafts = (List<Aircraft>) value.get(0);
                            Double coefficient = (Double) value.get(1);
                            if (coefficient == Double.NaN) {
                                return Traversers.empty();
                            }
                            String phase = coefficient > 0 ? "TAKING_OFF" : "LANDING";
                            Stream<Tuple2<Aircraft, String>> aircraftsWithPhase = aircrafts.stream().map(ac -> Tuple2.tuple2(ac, phase));
                            return Traversers.traverseStream(aircraftsWithPhase);
                        }
                )
        );


        Vertex enrichWithNoiseInfo = dag.newVertex("enrich with noise info",
                mapP(
                        (Tuple2<Aircraft, String> tuple2) -> {
                            Aircraft aircraft = tuple2.f0();
                            String phase = tuple2.f1();
                            Long altitude = aircraft.getAlt();
                            Integer currentDb = getPhaseNoiseLookupTable(phase, aircraft.getWtc()).tailMap(altitude.intValue()).values().iterator().next();
                            return Tuple3.tuple3(aircraft, phase, currentDb);

                        }
                )
        );

        Vertex averageNoise = dag.newVertex("average noise",
                aggregateToSlidingWindowP(
                        (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f0().getCity() + "_AVG_NOISE",
                        (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f0().getPosTime(),
                        TimestampKind.EVENT,
                        wDefTrend,
                        averagingLong(
                                (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f2()
                        )
                )
        );

        Vertex enrichWithC02Emission = dag.newVertex("enrich with C02 emission info",
                mapP(
                        (Tuple2<Aircraft, String> tuple2) -> {
                            Aircraft aircraft = tuple2.f0();
                            String phase = tuple2.f1();
                            Double ltoC02EmissionInKg = typeToLTOCycyleC02Emission.getOrDefault(aircraft.getType(), 0d);
                            return Tuple3.tuple3(aircraft, phase, ltoC02EmissionInKg);
                        }
                )
        );

        Vertex averagePollution = dag.newVertex("pollution", aggregateToSlidingWindowP(
                (Tuple3<Aircraft, String, Double> tuple3) -> tuple3.f0().getCity() + "_C02_EMISSION",
                (Tuple3<Aircraft, String, Double> tuple3) -> tuple3.f0().getPosTime(),
                TimestampKind.EVENT,
                wDefTrend,
                summingDouble(
                        (DistributedToDoubleFunction<Tuple3<Aircraft, String, Double>>) Tuple3::f2
                )
        ));

        Vertex emergencyMapSink = dag.newVertex("aircrafts in emergency to map",
                updateMapP("emergencyMap",
                        Aircraft::getId,
                        (Aircraft oldValue, Aircraft newValue) -> newValue)
        );

        Vertex takingOffMapSink = dag.newVertex("taking off aircrafts to map",
                updateMapP("takeOffMap",
                        (Tuple2<Aircraft, String> tuple2) -> tuple2.f0().getId(),
                        (Aircraft aircraft, Tuple2<Aircraft, String> tuple2) -> {
                            if ("TAKING_OFF".equals(tuple2.f1())) {
                                return tuple2.f0();
                            }
                            return null;
                        }
                )
        );

        Vertex landingMapSink = dag.newVertex("landing aircrafts to map",
                updateMapP("landingMap",
                        (Tuple2<Aircraft, String> tuple2) -> tuple2.f0().getId(),
                        (Aircraft aircraft, Tuple2<Aircraft, String> tuple2) -> {
                            if ("LANDING".equals(tuple2.f1())) {
                                return tuple2.f0();
                            }
                            return null;
                        }
                )
        );

        Vertex graphiteSink = dag.newVertex("graphite sink",
                ProcessorSupplier.of(
                        () -> new GraphiteSink("127.0.0.1", 2004)
                )
        );

        Vertex loggerSink = dag.newVertex("logger", DiagnosticProcessors.writeLoggerP());

        // planes emitting help/emergency signal
        dag.edge(from(source, 0).to(emittingHelpSignal));
        dag.edge(from(emittingHelpSignal).to(emergencyMapSink));

        // flights over interested cities
        dag.edge(from(source, 1).to(tagPlanesInInterestedCities));

        // flights less then altitude 3000 ft.
        dag.edge(from(tagPlanesInInterestedCities).to(altitudeLessThan3000ft));

        // identify flight phase (LANDING/TAKE-OFF)
        dag
                .edge(from(altitudeLessThan3000ft).to(insertWm))
                .edge(between(insertWm, trendStage1)
                        .partitioned(Aircraft::getId))
                .edge(between(trendStage1, trendStage2)
                        .partitioned((TimestampedEntry e) -> e.getKey()))
                .edge(from(trendStage2).to(identifyPhase).distributed());


        // average noise path
        dag
                .edge(from(identifyPhase, 0).to(enrichWithNoiseInfo))
                .edge(from(enrichWithNoiseInfo).to(averageNoise)
                                               .allToOne())
                .edge(from(averageNoise).to(graphiteSink));


        // C02 emission calculation path
        dag
                .edge(from(identifyPhase, 1).to(enrichWithC02Emission))
                .edge(from(enrichWithC02Emission).to(averagePollution)
                                                 .allToOne())
                .edge(from(averagePollution).to(graphiteSink, 1));

        // taking off planes to IMap
        dag.edge(from(identifyPhase, 2).to(takingOffMapSink));

        // landing planes to IMap
        dag.edge(from(identifyPhase, 3).to(landingMapSink));


        Job job = jet.newJob(dag);
        new Thread(() -> {
            while (true) {
                IStreamMap<Object, Object> takingOff = jet.getMap("takeOffMap");
                IStreamMap<Object, Object> landingMap = jet.getMap("landingMap");
                IStreamMap<Object, Object> emergencyMap = jet.getMap("emergencyMap");
                System.out.println("TAKING OFF");
                takingOff.entrySet().forEach(System.out::println);
                System.out.println("LANDING");
                landingMap.entrySet().forEach(System.out::println);
                System.out.println("EMERGENCY");
                emergencyMap.entrySet().forEach(System.out::println);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();
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

}
