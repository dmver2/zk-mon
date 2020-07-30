package home.dv.zkmon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class NioController implements Runnable, AutoCloseable {
    public static final int RECV_BUF = 0x800;
    public static final long REQ_PERIOD = 1000L;
    private static final long CONN_PERIOD = 5000L;
    private static final long SELECT_TIMEOUT = 500L;
    // number of requests to close socket after (0 - won't close)
    private static final int RESET_AFTER_QREQ = 0;

    private static final Logger LOG = LoggerFactory.getLogger(NioController.class);
    private final List<BasicTask> requests;
    private final NetStreamHandler handler = new HttpStreamHandler();
    private Map<Att, SelectableChannel> attachments;
    private Selector selector;

    NioController(final List<BasicTask> requests) {
        this.requests = requests;
    }

    private static void closeQuietly(final Closeable closeable) {
        try {
            LOG.trace("Application is about to close {}...", closeable);
            closeable.close();
        } catch (final IOException e) {
            LOG.error("FAILED CLOSING {}", closeable, e);
        }
    }

    @Override
    public void close() {
        Optional.ofNullable(selector).ifPresent(NioController::closeQuietly);
    }

    private void initConn(final Att attachment) throws IOException {
        final SocketChannel channel;
        channel = SocketChannel.open();
        final var rcvBufSize = channel.getOption(StandardSocketOptions.SO_RCVBUF);
        LOG.debug("recv buffer size: {} {} {}",
                rcvBufSize, (rcvBufSize < RECV_BUF) ? '<' : ">=",
                RECV_BUF);
        if (rcvBufSize < RECV_BUF) {
            channel.setOption(StandardSocketOptions.SO_RCVBUF, RECV_BUF);
            LOG.debug("{} new recv buffer size set to {} instead of {}",
                    attachment.task.url,
                    channel.getOption(StandardSocketOptions.SO_RCVBUF), rcvBufSize);
        }
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        channel.configureBlocking(false);
        channel.register(this.selector,
                SelectionKey.OP_CONNECT, attachment);
        channel.connect(attachment.address);
        attachments.put(attachment, channel);
        LOG.trace("attachments #{}", attachments.size());
    }

    private Att makeAttachment(final BasicTask task) {
        final byte[] messageBytes =
                task.request.getBytes(StandardCharsets.ISO_8859_1);
        final InetSocketAddress inetAddr = new InetSocketAddress(task.url.getHost(), task.url.getPort());
        return new Att(
                task,
                inetAddr,
                ByteBuffer.wrap(messageBytes),
                ByteBuffer.allocate(0x400),
                new StringBuilder(0x800)
        );
    }

    private void onConnect(final SelectionKey key) throws IOException {
        final SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {
            if (channel.finishConnect()) {
                LOG.debug("Connected to: {}", channel.getRemoteAddress());
                int ops = key.interestOps();
                ops &= (~SelectionKey.OP_CONNECT);
                ops |= SelectionKey.OP_WRITE;
                key.interestOps(ops);
                key.selector().wakeup();
            }
        }
    }

    private void renew(final Att attach, final SelectableChannel channel) {
        Optional.ofNullable(channel)
                .filter(AbstractInterruptibleChannel::isOpen)
                .ifPresent(NioController::closeQuietly);
        attach.error = null;
        attach.reset();
        try {
            initConn(attach);
        } catch (final IOException e) {
            LOG.error(String.format("ERROR RENEWING %s", attach.task.url), e);
        }
    }

    private void onError(final SelectionKey key, final Exception x) {
        final Att atta = (Att) key.attachment();
        cancelAndRelease(key);
        Optional.ofNullable(key.channel())
                .filter(AbstractInterruptibleChannel::isOpen)
                .ifPresent(NioController::closeQuietly);
        atta.error = x;
        atta.task.accept(atta);
        atta.reset();
        //            waitABit();
        try {
            initConn(atta);
        } catch (final IOException e) {
            LOG.error(String.format("ERROR CONNECTING %s AFTER ERROR", atta.task.url), e);
        }
    }

    private void read(final SelectionKey key) throws IOException {
        final Att attachment = (Att) key.attachment();
        // reset write state
        attachment.written = 0;

        final SocketChannel channel = (SocketChannel) key.channel();

        final ByteBuffer readBuffer = attachment.buffer; // ByteBuffer.allocate(0x400);
        readBuffer.clear();
        final int iobytes;
        try {
            iobytes = channel.read(readBuffer);
        } catch (final IOException e) {
            LOG.error("Reading problem {}, closing connection to {}",
                    getRequestUrl(key),
                    channel.getRemoteAddress(), e);
            cancelAndRelease(key);
            closeQuietly(channel);
            initConn(attachment);
            return;
        }
        if (iobytes == -1) {
            LOG.info("Nothing was read from server {} {}", getRequestUrl(key), channel.getRemoteAddress());
            cancelAndRelease(key);
            closeQuietly(channel);
            initConn(attachment);
            return;
        } else {
            LOG.trace("received: {} bytes", iobytes);
        }
        if (handler.readBytes(iobytes, attachment)) {
            attachment.task.accept(attachment);
            attachment.reset();

            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
            key.selector().wakeup();
        }
    }

    public void run() {
        try (final Selector s = Selector.open()) {
            selector = s;
            attachments = new HashMap<>(requests.size());
            for (final BasicTask task : requests) {
                final Att att = makeAttachment(task);
                initConn(att);
            }

            final long startTs = System.currentTimeMillis();
            int c = 0;
            while (!Thread.interrupted()) {
                //TODO: adjust select timeout
                final int selected = selector.select(SELECT_TIMEOUT);
                LOG.trace("selected: {}", selected);
                if (0 < selected) {
                    final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        final SelectionKey key = keys.next();
                        try {
                            keys.remove();
                            if (key.isValid()) {
                                if (key.isConnectable()) {
                                    onConnect(key);
                                } else if (key.isWritable()) {
                                    write(key);
                                } else if (key.isReadable()) {
                                    read(key);
                                } else {
                                    LOG.trace("cancelling unknown op_code key of {}", getRequestUrl(key));
                                    cancelAndRelease(key);
                                }
                            } else {
                                LOG.trace("cancelling invalid key of {}", getRequestUrl(key));
                                cancelAndRelease(key);
                            }
                        } catch (final IOException x) {
                            LOG.error(String.format("SELECTED KEY %s HANDLING ERROR", getRequestUrl(key)), x);
                            onError(key, x);
                        }
                    }
                } else {
                    final long cts = System.currentTimeMillis();
                    if (startTs + c * REQ_PERIOD <= cts) {
                        ++c;
                        final boolean renew = (0 != RESET_AFTER_QREQ) && (0 == (c % RESET_AFTER_QREQ));
                        // waitABit(5000L + ts - System.currentTimeMillis());
                        this.attachments.forEach((attachment, channel) -> {
                            /* connected */
                            if (channel.isOpen()
                                    && ((SocketChannel) channel).isConnected()) {
                                if (renew) {
                                    renew(attachment, channel);
                                } else {
                                    final long elapsedMs = System.currentTimeMillis() - attachment.ts;
                                    if (elapsedMs >= REQ_PERIOD) {
                                        LOG.debug("Time to request {} after idle timeout: {}",
                                                attachment.task.url, elapsedMs);
                                        attachment.ts = System.currentTimeMillis();
                                        final Optional<SelectionKey> foundKey = selector.keys().stream()
                                                .filter(k -> k.attachment() == attachment).findAny();
                                        foundKey.ifPresentOrElse(key -> {
                                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                            key.selector().wakeup();
                                        }, () -> reconnect(attachment));
                                    }
                                }
                            } else {
                                reconnect(attachment);
                            }
                        });
                    }
                }
            }
        } catch (final IOException x) {
            LOG.error("ERROR", x);
        } finally {
            selector = null;
        }
    }

    private void cancelAndRelease(final SelectionKey key) {
        key.attach(null);
        key.cancel();
    }

    private void reconnect(final Att attachment) {
        final long elapsedMs = System.currentTimeMillis() - attachment.ts;
        if (elapsedMs >= CONN_PERIOD) {
            LOG.debug("Time to connect {} after error timeout: {}",
                    attachment.task.url, elapsedMs);
            try {
                initConn(attachment);
            } catch (final IOException e) {
                LOG.error(String.format("ERROR ATTEMPTING CONNECT %s after timeout", attachment.task.url), e);
            }
        }
    }

    private URL getRequestUrl(final SelectionKey key) {
        return ((Att) key.attachment()).task.url;
    }

    private boolean write(final SelectionKey key) throws IOException {
        final Att attachment = (Att) key.attachment();
        if (attachment.writeCompleted()) {
            return true;
        }

        // reset read state
        attachment.response.delete(0, attachment.response.length());
        attachment.received = 0;

        final SocketChannel channel = (SocketChannel) key.channel();
        final ByteBuffer buffer = attachment.msgBuffer;

        final int written = channel.write(buffer);
        attachment.written += written;

        LOG.debug("url: {} addr: {} WRITTEN {} bytes ({} {})",
                attachment.task.url,
                channel.getRemoteAddress(),
                attachment.written,
                attachment.written == buffer.limit() ? "all of" : "less than",
                buffer.limit());
        // let's get ready to read.
        if (attachment.writeCompleted()) {
            attachment.written = 0;
            buffer.rewind();
            key.interestOps(
                    (key.interestOps() & (~SelectionKey.OP_WRITE)) | SelectionKey.OP_READ);
            key.selector().wakeup();
            return true;
        }
        return false;
    }

    public static final class Att {
        final SocketAddress address;
        final ByteBuffer msgBuffer;
        final ByteBuffer buffer;
        final StringBuilder response;
        final BasicTask task;
        int contentLength;
        boolean chunked;
        Throwable error;
        int headerLength;
        int received;
        long ts;
        int written;

        Att(final BasicTask task, final SocketAddress address,
            final ByteBuffer aMsgBuffer,
            final ByteBuffer buffer,
            final StringBuilder stringBuilder) {
            this.task = task;
            this.msgBuffer = aMsgBuffer;
            this.buffer = buffer;
            this.address = address;
            this.response = stringBuilder;
            this.ts = System.currentTimeMillis();
        }

        public void reset() {
            contentLength = 0;
            chunked = false;
            error = null;
            headerLength = 0;
            received = 0;
            response.delete(0, response.length());
            response.setLength(0x800);
            ts = System.currentTimeMillis();
            written = 0;
        }

        public boolean writeCompleted() {
            return written >= msgBuffer.limit();
        }

        @Override
        public String toString() {
            final StringBuilder sb =
                    new StringBuilder(
                            0x200 + Optional.ofNullable(this.response).map(e -> e.length())
                                    .orElse(0));
            sb.append("Att{");
            sb.append(", url=").append(task.url);
            sb.append(", address=").append(address);
            sb.append(", buffer=").append(buffer);
            sb.append(", read=").append(received);
            sb.append(", written=").append(written);
            sb.append(", chunked=").append(chunked);
            sb.append(", contentLength=").append(contentLength);
            sb.append(", error=").append(error);
            sb.append(", headerLength=").append(headerLength);
            sb.append(", response=").append(sb);
            sb.append('}');
            return sb.toString();
        }

        public String getPayload() {
            return Optional.ofNullable(response).map(s -> s.substring(headerLength))
                    .orElse("");
        }

    }
}
