package home.dv.nio.js;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.Iterator;
import java.util.Optional;

public class JsonStreamingTest {

    public static void main(final String[] args) throws Exception {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream("out/test/resources/mntr.json")))) {
            final FieldsJsonIterator fieldsJsonIterator = new FieldsJsonIterator(reader);
            while (fieldsJsonIterator.hasNext()) {
                final Fields fields = fieldsJsonIterator.next();
                System.out.println(fields);
                // Save object to DB
            }
        }
    }
}

class FieldsJsonIterator implements Iterator<Fields> {

    private final ObjectMapper mapper;
    private final JsonParser parser;

    public FieldsJsonIterator(final Reader reader) throws IOException {
        mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        parser = mapper.getFactory().createParser(reader);
        skipStart();
    }

    private void skipStart() throws IOException {
        while (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.nextToken();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            while (parser.currentToken() == null) {
                parser.nextToken();
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return parser.currentToken() == JsonToken.START_OBJECT;
    }

    @Override
    public Fields next() {
        try {
            final JsonNode v = mapper.readValue(parser, JsonNode.class);
            return new Fields(
                    Optional.ofNullable(v.get("server_state")).map(JsonNode::asText).orElse(null),
                    Optional.ofNullable(v.get("error")).map(JsonNode::asText).orElse(null));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

class Fields {
    @JsonProperty("error")
    private final String error;

    @JsonProperty("server_state")
    private final String serverState;

    public Fields(final String serverState, final String error) {
        this.serverState = serverState;
        this.error = error;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(0x40);
        sb.append("Fields{");
        sb.append("serverState='").append(serverState).append('\'');
        sb.append(", error='").append(error).append('\'');
        sb.append('}');
        return sb.toString();
    }
}