package home.dv.zkmon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.function.Consumer;

public class BasicTask implements Consumer<NioController.Att> {
    private static final Logger LOG = LoggerFactory.getLogger(BasicTask.class);

    final URL url;
    final String request;

    public BasicTask(final URL url, final String request) {
        this.url = url;
        this.request = request;
    }

    @Override
    public void accept(final NioController.Att att) {
        LOG.debug("u: {} {}", url, att.toString());
    }

    @Override
    public String toString() {
        return "BasicTask{" +
                "url=" + url +
                '}';
    }
}
