package com.hazelcast.jet.demo;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.modules.cPickle;

/**
 * date: 1/16/18
 * author: emindemirci
 */
public class GraphiteSink extends AbstractProcessor {

    private final String host;
    private final int port;
    private OutputStream outputStream;

    public GraphiteSink(String host, int port) {
        this.host = host;
        this.port = port;

    }

    @Override
    protected void init(Context context) throws Exception {
        Socket socket = new Socket(host, port);
        outputStream = socket.getOutputStream();

    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception {
        PyList list = new PyList();
        PyString metricName;
        PyInteger timestamp;
        PyFloat metricValue;
        if (item instanceof TimestampedEntry) {
            TimestampedEntry<String, Double> timestampedEntry = (TimestampedEntry) item;
            metricName = new PyString(timestampedEntry.getKey());
            timestamp = new PyInteger((int) Instant.ofEpochMilli(timestampedEntry.getTimestamp()).getEpochSecond());
            metricValue = new PyFloat(timestampedEntry.getValue());
        } else if (item instanceof Tuple2) {
            Tuple2<Aircraft, String> aircraftWithPhase = (Tuple2<Aircraft, String>) item;
            metricName = new PyString(aircraftWithPhase.f0().getCity() + "." + aircraftWithPhase.f1());
            timestamp = new PyInteger((int) Instant.ofEpochMilli(aircraftWithPhase.f0().getPosTime()).getEpochSecond());
            metricValue = new PyFloat(1);
        } else {
            return true;
        }
        PyTuple metric = new PyTuple(metricName, new PyTuple(timestamp, metricValue));
        list.add(metric);
        System.out.println("added metric = " + metric);

        PyString payload = cPickle.dumps(list, 2);
        byte[] header = ByteBuffer.allocate(4).putInt(payload.__len__()).array();

        outputStream.write(header);
        outputStream.write(payload.toBytes());
        outputStream.flush();
        return true;
    }

    @Override
    public boolean complete() {
        return false;
    }
}
