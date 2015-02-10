package org.keedio.flume.interceptor.collector;

import com.google.common.base.Preconditions;
import org.apache.avro.AvroRemoteException;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Freely taken from:
 * https://github.com/apache/flume/blob/trunk/flume-ng-embedded-agent/src/test/java/org/apache/flume/agent/embedded/TestEmbeddedAgent.java
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 10/2/15.
 */
public class EventCollector implements AvroSourceProtocol {
    private final Queue<AvroFlumeEvent> eventQueue =
            new LinkedBlockingQueue<AvroFlumeEvent>();
    public Event poll() {
        AvroFlumeEvent avroEvent = eventQueue.poll();
        if(avroEvent != null) {
            return EventBuilder.withBody(avroEvent.getBody().array(),
                    toStringMap(avroEvent.getHeaders()));
        }
        return null;
    }
    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
        eventQueue.add(event);
        return Status.OK;
    }
    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
            throws AvroRemoteException {
        Preconditions.checkState(eventQueue.addAll(events));
        return Status.OK;
    }

    private static Map<String, String> toStringMap(
            Map<CharSequence, CharSequence> charSeqMap) {
        Map<String, String> stringMap =
                new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }
}
