package home.dv.zkmon;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Nio1WithProto {
    private static final Logger LOG = LoggerFactory.getLogger(Nio1WithProto.class);

    /**
     * @param args command line: zookeeper url list
     */
    public static void main(final String[] args) {

        LOG.info("nio1 requests to: {}", Arrays.toString(args));

        final List<BasicTask> requests = Stream.of(args)
                .map(arg ->
                        {
                            final URL url;
                            try {
                                url = new URL(arg);
                            } catch (final MalformedURLException e) {
                                throw new Error(e);
                            }
                            final int port = (-1 != url.getPort()) ? url.getPort() : url.getDefaultPort();
                            switch (port) {
                                case 8080:
                                    return new ZkTaskImpl(url,
                                            String.format("GET %s HTTP/1.1%n"
                                                    + "Host: %s:%d%n"
                                                    + "Connection: Keep-Alive%n"
                                                    + "Cache-Control:max-age=0"
                                                    + "Accept: */*%n%n", url.getPath(), url.getHost(), port)
                                    );
                                default:
                                    return new BasicTask(url,
                                            String.format("GET %s HTTP/1.1%n"
                                                    + "Host: %s:%d%n"
                                                    + "Connection: Keep-Alive%n"
                                                    + "Cache-Control:max-age=0"
                                                    + "Accept: */*%n%n", url.getPath(), url.getHost(), port)
                                    );
                            }
                        }
                ).collect(Collectors.toList());

        final NioController nioController = new NioController(requests);
        nioController.run();
    }

}