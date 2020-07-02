package home.dv.zkmon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class BasicTask implements Consumer<NioController.Att> {
    private static final Logger LOG = LoggerFactory.getLogger(BasicTask.class);

    final InetSocketAddress address;
    final String request;

    public BasicTask(final InetSocketAddress address, final String request) {
        this.address = address;
        this.request = request;
    }

    @Override
    public void accept(final NioController.Att att) {
        LOG.debug("%s", att);
    }
}
