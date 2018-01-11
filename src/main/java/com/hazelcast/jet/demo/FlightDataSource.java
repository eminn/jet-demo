package com.hazelcast.jet.demo;


import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonArray;
import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.util.ExceptionUtil;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;

/**
 * date: 1/8/18
 * author: emindemirci
 */
public class FlightDataSource extends AbstractProcessor {

    private final URL url;
    private final long intervalMillis;
    private Traverser<Aircraft> aircraftTraverser;
    private boolean lastTraverserExhausted = true;

    public FlightDataSource(String url, long intervalMillis) {
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            throw ExceptionUtil.rethrow(e);
        }
        this.intervalMillis = intervalMillis;
    }

    @Override protected void init(Context context) throws Exception {
        super.init(context);
    }


    @Override public boolean complete() {
        System.out.println("FlightDataSource.complete");
        if (lastTraverserExhausted){
            poll();
        }
        lastTraverserExhausted = emitFromTraverser(aircraftTraverser);
        System.out.println("lastTraverserExhausted = " + lastTraverserExhausted);
        return false;
    }

    private void poll() {
        System.out.println("FlightDataSource.poll");
        try {
            HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

            con.setRequestMethod("GET");
            con.addRequestProperty("User-Agent", "Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 40.0.2214.91 Safari / 537.36");
            con.getResponseCode();

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            con.disconnect();

            JsonValue value = Json.parse(response.toString());
            JsonObject object = value.asObject();
            JsonArray acList = object.get("acList").asArray();
            aircraftTraverser = Traversers.traverseStream(acList.values().stream().map(ac -> {
                        Aircraft aircraft = new Aircraft();
                        aircraft.fromJson(ac.asObject());
                        return aircraft;
                    })
            );
            System.out.println("aircraftTraverser = " + aircraftTraverser);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static ProcessorMetaSupplier supplier(String url, long intevalMillis) {
        return dontParallelize(() -> new FlightDataSource(url, intevalMillis));
    }

}
