package home.dv.zkmon;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Nio1WithProto {
    private static final Logger LOG = LoggerFactory.getLogger(Nio1WithProto.class);

    /**
     * @param args
     */
    public static void main(final String[] args) {

        LOG.info("nio1 requests to: {}", Arrays.toString(args));

        final List<BasicTask> requests = Stream.of(args)
                .map(arg ->
                        {
                            final String[] lexemes = arg.split(":");
                            final String host = lexemes[0];
                            final int port = Integer.parseInt(lexemes[1]);
                            switch (port) {
                                case 8080:
                                    return new ZkTaskImpl(new InetSocketAddress(host, port),
                                            String.format("GET /commands/mntr HTTP/1.1%n"
                                                    + "Host: %s:%d%n"
                                                    + "Connection: Keep-Alive%n"
                                                    + "Cache-Control:max-age=0"
                                                    + "Accept: */*%n%n", host, port)
                                    );
                                default:
                                    return new BasicTask(new InetSocketAddress(host, port),
                                            String.format("GET / HTTP/1.1%n"
                                                    + "Host: %s:%d%n"
                                                    + "Connection: Keep-Alive%n"
                                                    + "Cache-Control:max-age=0"
                                                    + "Accept: */*%n%n", host, port)
                                    );
                            }
                        }
                ).collect(Collectors.toList());

        final NioController nioController = new NioController(requests);
        nioController.run();
    }

}