package org.apache.flume;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MessagePackToJSONInterceptor implements Interceptor {
    private final Logger log = Logger.getLogger(MessagePackToJSONInterceptor.class);

    public static final byte[] NEWLINE_BYTES = "\n".getBytes();
    private static final TypeReference<Object> typeReference = new TypeReference<Object>() {};

    private ObjectMapper msgpMapper;
    private ObjectMapper jsonMapper;

    public void initialize() {
        msgpMapper = new ObjectMapper(new MessagePackFactory());
        jsonMapper = new ObjectMapper();
    }

    public Event intercept(Event event) {
        InputStream is = new ByteArrayInputStream(event.getBody());
        StringWriter stringWriter = new StringWriter();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            JsonParser parser = msgpMapper.getFactory().createParser(is);
            Iterator<Object> objectIterator = parser.readValuesAs(typeReference);
            while (objectIterator.hasNext()) {
                Object next = objectIterator.next();
                jsonMapper.writeValue(os, next);
                os.write(NEWLINE_BYTES);
            }
            event.setBody(os.toByteArray());
        } catch (IOException e) {
            log.error(e);
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        if (list == null) {
            return list;
        }
        List<Event> result = new ArrayList<Event>(list.size());
        for (int i = 0; i < list.size(); i++) {
            result.add(intercept(result.get(i)));
        }
        return result;
    }

    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new MessagePackToJSONInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
