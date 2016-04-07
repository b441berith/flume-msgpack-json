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
import java.util.zip.GZIPInputStream;

public class MessagePackToJSONInterceptor implements Interceptor {
    private final Logger log = Logger.getLogger(MessagePackToJSONInterceptor.class);

    private static final byte[] NEW_LINE_BYTES = System.getProperty("line.separator").getBytes();

    private static final TypeReference<Object> typeReference = new TypeReference<Object>() {};

    private ObjectMapper msgpMapper;
    private ObjectMapper jsonMapper;

    public void initialize() {
        msgpMapper = new ObjectMapper(new MessagePackFactory());
        jsonMapper = new ObjectMapper();
    }

    public static InputStream getInputStream(InputStream input) throws IOException {
        PushbackInputStream pb = new PushbackInputStream(input, 2);
        byte[] signature = new byte[2];
        pb.read(signature);
        pb.unread(signature);
        if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
            return new GZIPInputStream(pb);
        } else {
            return pb;
        }
    }

    public Event intercept(Event event) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            InputStream is = getInputStream(new ByteArrayInputStream(event.getBody()));
            JsonParser parser = msgpMapper.getFactory().createParser(is);
            Iterator<Object> objectIterator = parser.readValuesAs(typeReference);
            while (objectIterator.hasNext()) {
                Object next = objectIterator.next();
                jsonMapper.writeValue(os, next);
                boolean hasMoreRows = false;
                try {
                    hasMoreRows = objectIterator.hasNext();
                } catch (Exception ignored) {
                }
                if (hasMoreRows) {
                    os.write(NEW_LINE_BYTES);
                }
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
            result.add(intercept(list.get(i)));
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
