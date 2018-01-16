package com.hazelcast.jet.demo;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.TimestampedEntry;
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
        TimestampedEntry<String, Double> timestampedEntry = (TimestampedEntry) item;
        PyList list = new PyList();
        PyString metricName = new PyString(timestampedEntry.getKey());
        PyInteger timestamp = new PyInteger((int)Instant.ofEpochMilli(timestampedEntry.getTimestamp()).getEpochSecond());
        PyFloat metricValue = new PyFloat(timestampedEntry.getValue());
        PyTuple metric = new PyTuple(metricName, new PyTuple(timestamp, metricValue));
        list.add(metric);
        System.out.println("added metric = " + metric);

        PyString payload = cPickle.dumps(list,2);
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
