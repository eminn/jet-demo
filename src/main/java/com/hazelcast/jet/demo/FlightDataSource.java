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
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static java.net.URLEncoder.encode;

/**
 * date: 1/8/18
 * author: emindemirci
 */
public class FlightDataSource extends AbstractProcessor {

    private final URL url;
    private final long intervalMillis;
    private Traverser<Aircraft> aircraftTraverser;
    private boolean lastTraverserExhausted = true;
    private List<String> knownIds = new ArrayList<>(10000);
    private int[] count;

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
        if (lastTraverserExhausted) {
            poll();
        }
        lastTraverserExhausted = emitFromTraverser(aircraftTraverser);
        return false;
    }

    private void poll() {
        try {
            String ids = knownIds.stream().collect(Collectors.joining("-"));
            byte[] out = (encode("ICAOS", "UTF-8") + "=" + encode(ids, "UTF-8")).getBytes(StandardCharsets.UTF_8);
            int length = out.length;

            HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.addRequestProperty("User-Agent", "Mozilla / 5.0 (Windows NT 6.1; WOW64) AppleWebKit / 537.36 (KHTML, like Gecko) Chrome / 40.0.2214.91 Safari / 537.36");
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            con.setRequestProperty("Content-Length", String.valueOf(length));

            try (OutputStream os = con.getOutputStream()) {
                os.write(out);
            }

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
                        knownIds.add(aircraft.getIcao());
                        return aircraft;
                    })
            );
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static ProcessorMetaSupplier supplier(String url, long intevalMillis) {
        return dontParallelize(() -> new FlightDataSource(url, intevalMillis));
    }

}
