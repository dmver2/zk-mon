package home.dv.zkmon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Optional;

public class ZkTaskImpl extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(ZkTaskImpl.class);
    final ObjectMapper jsonMapper;

    public ZkTaskImpl(final InetSocketAddress address, final String request) {
        super(address, request);
        jsonMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void accept(final NioController.Att att) {
        try {
            if (null != att.error) {
                LOG.error("net ERROR: {}", att.error.toString());
            } else {
                final JsonNode jsonNode = jsonMapper.readTree(att.getPayload());
                final String serverState = jsonNode.get("server_state").asText();
                final Optional<String> error = Optional.ofNullable(jsonNode.get("error").asText())
                        .map(s -> (0 == s.length()) ? null : s)
                        .map(s -> ("null".equals(s) ? null : s));
                LOG.info("zk state from JSON: s: [{}] e: [{}]", serverState, error);
                if (error.isEmpty()) {
                    switch (serverState) {
                        case "follower":
                            LOG.info("follower - OK");
                            break;
                        case "leader":
                            LOG.info("leader - OK");
                            break;
                        case "standalone":
                            LOG.info("standalone - OK");
                            break;
                        default:
                            LOG.warn("unknown state: {}", serverState);
                    }
                } else {
                    LOG.error("zk ERROR: {}", error);
                }
            }
        } catch (final JsonProcessingException e) {
            LOG.error("JSON parsing FAILED", e);
        }
    }
}
