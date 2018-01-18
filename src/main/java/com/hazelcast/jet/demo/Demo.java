package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.demo.types.WakeTurbulanceCategory;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.map.listener.EntryAddedListener;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLagAndDelay;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekInputP;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.mergeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.demo.types.WakeTurbulanceCategory.HEAVY;
import static com.hazelcast.jet.demo.util.Util.inAtlanta;
import static com.hazelcast.jet.demo.util.Util.inFrankfurt;
import static com.hazelcast.jet.demo.util.Util.inIstanbul;
import static com.hazelcast.jet.demo.util.Util.inLondon;
import static com.hazelcast.jet.demo.util.Util.inParis;
import static java.util.stream.Collectors.joining;

public class Demo {

    private static final String SOURCE_URL = "https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json";
    private static final String TAKE_OFF_MAP = "takeOffMap";
    private static final String LANDING_MAP = "landingMap";
    private static final String HELP_SIGNAL_MAP = "helpSignalMap";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();

        DAG dag = buildDAG();

        addListener(jet.getMap(TAKE_OFF_MAP), a -> System.out.println("New aircraft taking off: " + a));
        addListener(jet.getMap(LANDING_MAP), a -> System.out.println("New aircraft landing " + a));
        addListener(jet.getMap(HELP_SIGNAL_MAP), a -> System.out.println("New aircraft requesting help: " + a));

        try {
            Job job = jet.newJob(dag);
            job.join();
        } finally {
            Jet.shutdownAll();
        }
    }


    private static DAG buildDAG() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("flightDataSource", FlightDataSource.supplier(SOURCE_URL, 10000));
        // we are interested only in planes above the ground
        Vertex filterAboveGround = dag.newVertex("filterAboveGround", filterP((Aircraft ac) -> !ac.isGnd()))
                                      .localParallelism(1);
        Vertex assignCity = dag.newVertex("assignCity", mapP(Demo::assignCity)).localParallelism(1);

        //altitude less than 3000
        Vertex filterLowAltitude = dag.newVertex("filterLowAltitude", filterP(
                (Aircraft ac) -> ac.getAlt() > 0 && ac.getAlt() < 3000)
        ).localParallelism(1);

        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(60_000, 30_000);
        DistributedSupplier<Processor> insertWMP = insertWatermarksP(wmGenParams(
                Aircraft::getPosTime,
                limitingLagAndDelay(TimeUnit.MINUTES.toMillis(15), 10_000),
                emitByFrame(wDefTrend),
                60000L
        ));
        Vertex insertWm = dag.newVertex("insertWm", peekOutputP(
                Object::toString,
                o -> o instanceof Watermark,
                insertWMP)).localParallelism(1);

        // aggregation: calculate altitude trend
        Vertex aggregateTrend = dag.newVertex("aggregateTrend", aggregateToSlidingWindowP(
                Aircraft::getId,
                Aircraft::getPosTime,
                TimestampKind.EVENT,
                wDefTrend,
                allOf(toList(), linearTrend(Aircraft::getPosTime, Aircraft::getAlt))
        ));

        // tag and filter aircraft with flight phase
        Vertex tagWithPhase = dag.newVertex("tagWithPhase", mapP(
                (TimestampedEntry<Long, List<Object>> entry) -> {
                    List<Object> results = entry.getValue();
                    List<Aircraft> aircraftList = (List<Aircraft>) results.get(0);
                    Aircraft aircraft = aircraftList.get(0);
                    double coefficient = (double) results.get(1);
                    String phase = getPhase(coefficient);
                    if (phase == null) {
                        return null;
                    }

                    return tuple2(aircraft, phase);
                }
        ));
//
        Vertex enrichWithNoiseInfo = dag.newVertex("enrich with noise info", mapP(
                (Tuple2<Aircraft, String> tuple2) -> {
                    Aircraft aircraft = tuple2.f0();
                    String phase = tuple2.f1();
                    Long altitude = aircraft.getAlt();
                    Integer currentDb = getPhaseNoiseLookupTable(phase, aircraft.getWtc()).tailMap(altitude.intValue()).values().iterator().next();
                    return Tuple3.tuple3(aircraft, phase, currentDb);

                })
        );

        Vertex averageNoise = dag.newVertex("average noise", aggregateToSlidingWindowP(
                (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f0().getCity() + "_AVG_NOISE",
                (Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f0().getPosTime(),
                TimestampKind.EVENT,
                wDefTrend,
                AggregateOperations.averagingLong((Tuple3<Aircraft, String, Integer> tuple3) -> tuple3.f2())
        ));

        Vertex enrichWithC02Emission = dag.newVertex("enrich with C02 emission info", mapP(
                (Tuple2<Aircraft, String> tuple2) -> {
                    Aircraft aircraft = tuple2.f0();
                    String phase = tuple2.f1();
                    Double ltoC02EmissionInKg = Constants.typeToLTOCycyleC02Emission.getOrDefault(aircraft.getType(), 0d);
                    return Tuple3.tuple3(aircraft, phase, ltoC02EmissionInKg);
                }
        ));

        Vertex averagePollution = dag.newVertex("pollution", peekInputP(aggregateToSlidingWindowP(
                (Tuple3<Aircraft, String, Double> tuple3) -> tuple3.f0().getCity() + "_C02_EMISSION",
                (Tuple3<Aircraft, String, Double> tuple3) -> tuple3.f0().getPosTime(),
                TimestampKind.EVENT,
                wDefTrend,
                summingDouble(
                        (DistributedToDoubleFunction<Tuple3<Aircraft, String, Double>>) Tuple3::f2
                )
        )));

        Vertex filterHelpSignal = dag.newVertex("filterHelpSignal", filterP(Aircraft::isHelp))
                                     .localParallelism(1);
        Vertex emergencyMapSink = dag.newVertex("helpSignalMapSink",
                mergeMapP(HELP_SIGNAL_MAP,
                        Aircraft::getId,
                        DistributedFunction.identity(),
                        (l, r) -> r
                ));

        Vertex takeOffMapSink = dag.newVertex("takeOffMapSink",
                updateMapP(TAKE_OFF_MAP,
                        (Tuple2<Aircraft, String> tuple2) -> tuple2.f0().getId(),
                        (Aircraft aircraft, Tuple2<Aircraft, String> tuple2) -> {
                            if ("TAKING_OFF".equals(tuple2.f1())) {
                                return tuple2.f0();
                            }
                            return null;
                        }
                )
        );

        Vertex landingMapSink = dag.newVertex("landingMapSink",
                updateMapP(LANDING_MAP,
                        (Tuple2<Aircraft, String> tuple2) -> tuple2.f0().getId(),
                        (Aircraft aircraft, Tuple2<Aircraft, String> tuple2) -> {
                            if ("LANDING".equals(tuple2.f1())) {
                                return tuple2.f0();
                            }
                            return null;
                        }
                )
        );
//
        Vertex graphiteSink = dag.newVertex("graphite sink",
                ProcessorSupplier.of(
                        () -> new GraphiteSink("127.0.0.1", 2004)
                )
        );

        Vertex loggerSink = dag.newVertex("logger", DiagnosticProcessors.writeLoggerP());

        dag.edge(between(source, filterAboveGround));

        // flights over interested cities and less then altitude 3000ft.
        dag.edge(between(filterAboveGround, filterLowAltitude))
           .edge(between(filterLowAltitude, assignCity));

        // identify flight phase (LANDING/TAKE-OFF)
        dag
                .edge(between(assignCity, insertWm))
                .edge(between(insertWm, aggregateTrend).distributed().partitioned(Aircraft::getId))
                .edge(between(aggregateTrend, tagWithPhase));

//         average noise path
        dag
                .edge(from(tagWithPhase, 0).to(enrichWithNoiseInfo))
                .edge(between(enrichWithNoiseInfo, averageNoise).distributed().allToOne())
                .edge(between(averageNoise, graphiteSink));


        // C02 emission calculation path
        dag
                .edge(from(tagWithPhase, 1).to(enrichWithC02Emission))
                .edge(between(enrichWithC02Emission, averagePollution).distributed().allToOne())
                .edge(from(averagePollution).to(graphiteSink, 1));

        // taking off planes to IMap
        dag.edge(from(tagWithPhase, 2).to(takeOffMapSink));

        // landing planes to IMap
        dag.edge(from(tagWithPhase, 3).to(landingMapSink));

        dag.edge(from(tagWithPhase, 4).to(graphiteSink,2));

        dag.edge(from(filterAboveGround, 1).to(filterHelpSignal))
           .edge(between(filterHelpSignal, emergencyMapSink));


//        dag.edge(from(tagWithPhase, 2).to(loggerSink));
        return dag;
    }

    private static Aircraft assignCity(Aircraft ac) {
        if (ac.getAlt() > 0 && !ac.isGnd()) {
            String city = getCity(ac.lon, ac.lat);
            if (city == null) {
                return null;
            }
            ac.setCity(city);
        }
        return ac;
    }

    private static String getPhase(double coefficient) {
        if (coefficient == Double.NaN) {
            return null;
        }
        return coefficient > 0 ? "TAKING_OFF" : "LANDING";
    }

    private static String getCity(float lon, float lat) {
        if (inLondon(lon, lat)) {
            return "London";
        } else if (inIstanbul(lon, lat)) {
            return "Istanbul";
        } else if (inFrankfurt(lon, lat)) {
            return "Frankfurt";
        } else if (inAtlanta(lon, lat)) {
            return "Atlanta";
        } else if (inParis(lon, lat)) {
            return "Paris";
        }
        // unknown city
        return null;
    }

    private static SortedMap<Integer, Integer> getPhaseNoiseLookupTable(String phase, WakeTurbulanceCategory wtc) {
        if ("TAKING_OFF".equals(phase)) {
            if (HEAVY.equals(wtc)) {
                return Constants.heavyWTCClimbingAltitudeToNoiseDb;
            } else {
                return Constants.mediumWTCClimbingAltitudeToNoiseDb;
            }
        } else if ("LANDING".equals(phase)) {
            if (HEAVY.equals(wtc)) {
                return Constants.heavyWTCDescendAltitudeToNoiseDb;
            } else {
                return Constants.mediumWTCDescendAltitudeToNoiseDb;
            }
        }
        return Collections.emptySortedMap();
    }

    private static void addListener(IStreamMap<Long, Aircraft> map, Consumer<Aircraft> consumer) {
        map.addEntryListener((EntryAddedListener<Long, Aircraft>) event -> consumer.accept(event.getValue()), true);
    }

}
