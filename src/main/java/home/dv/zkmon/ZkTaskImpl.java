package home.dv.zkmon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Optional;
import java.util.Scanner;

public class ZkTaskImpl extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(ZkTaskImpl.class);
    final ObjectMapper jsonMapper;

    public ZkTaskImpl(final URL address, final String request) {
        super(address, request);
        jsonMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void accept(final NioController.Att att) {
        try {
            final long lag = System.currentTimeMillis() - att.ts;
            if (null != att.error) {
                LOG.error("service: {} lag: {} ms; net ERROR: {}", url, lag, att.error.toString());
            } else {
                final String contentType = getContentType(att);
                if (!contentType.contains("/json")) {
                    LOG.error("url: {}, lag: {} ms; zk ERROR: expected json, returned {}", url, lag, contentType);
                    return;
                }

                final JsonNode jsonNode = jsonMapper.readTree(att.getPayload());
                final Optional<String> serverState = Optional.ofNullable(jsonNode.get("server_state"))
                        .map(JsonNode::asText);
                final Optional<String> error = Optional.ofNullable(jsonNode.get("error"))
                        .map(JsonNode::asText)
                        .map(s -> (0 == s.length()) ? null : s)
                        .map(s -> ("null".equals(s) ? null : s));
                LOG.info("service: {} lag: {} ms; zk state from JSON: s: [{}] e: [{}]", url, lag, serverState, error);
                if (error.isEmpty()) {
                    switch (serverState.orElse("N/A")) {
                        case "follower":
                            LOG.info("{} follower - OK", url);
                            break;
                        case "leader":
                            LOG.info("{} leader - OK", url);
                            break;
                        case "standalone":
                            LOG.info("{} standalone - OK", url);
                            break;
                        default:
                            LOG.warn("{} unknown state: {}", url, serverState);
                    }
                } else {
                    LOG.error("{} zk ERROR: {}", url, error);
                }
            }
        } catch (final JsonProcessingException e) {
            LOG.error("{} JSON parsing FAILED", url, e);
        }
    }

    private String getContentType(final NioController.Att att) {
        String contentType = null;
        final Scanner scanner = new Scanner(att.response.substring(0, att.headerLength)).useDelimiter("\r\n");
        while (scanner.hasNext()) {
            final String head = scanner.next();
            final String prefix = "Content-Type: ";
            if (head.startsWith(prefix)) {
                contentType = head.substring(prefix.length());
                break;
            }
        }
        return contentType;
    }
}
