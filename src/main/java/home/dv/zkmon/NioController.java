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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class NioController implements Runnable, AutoCloseable {
    public static final int RECV_BUF = 0x8000;
    public static final long REQ_PERIOD = 5000L;
    private static final Logger LOG = LoggerFactory.getLogger(NioController.class);
    private final Object objLock = new Object();
    private final List<BasicTask> requests;
    private final NetStreamHandler handler = new HttpStreamHandler();
    private List<Att> attachments;
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
        closeQuietly(selector);
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
        channel.configureBlocking(false);
        channel.register(this.selector,
                SelectionKey.OP_CONNECT, attachment);
        channel.connect(attachment.address);
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

    private void onError(final SelectionKey key, final Exception x) {
        key.cancel();
        Optional.ofNullable(key.channel())
                .filter(AbstractInterruptibleChannel::isOpen)
                .ifPresent(NioController::closeQuietly);
        final Att atta = (Att) key.attachment();
        atta.error = x;
        atta.task.accept(atta);
        atta.reset();
        atta.ts = System.currentTimeMillis();
        //            waitABit();
        try {
            initConn(atta);
        } catch (final IOException e) {
            LOG.error(String.format("ERROR CONNECTING %s AFTER ERROR", atta.task.url), e);
        }
    }

    private void read(final SelectionKey key) throws IOException {
        final Att attachment = (Att) key.attachment();
        if (attachment.readCompleted()) {
            return;
        }
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
            key.cancel();
            closeQuietly(channel);
            initConn(attachment);
            return;
        }
        if (iobytes == -1) {
            LOG.info("Nothing was read from server {}", channel.getRemoteAddress());
            key.cancel();
            closeQuietly(channel);
            initConn(attachment);
            return;
        } else {
            LOG.trace("received: {} bytes", iobytes);
        }
        if (handler.readBytes(iobytes, attachment)) {
            attachment.task.accept(attachment);
            attachment.ts = System.currentTimeMillis();
            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
            key.selector().wakeup();
        }
    }

    public void run() {
        try {
            selector = Selector.open();
            attachments = new ArrayList<>(requests.size());
            for (final BasicTask task : requests) {
                final Att att = makeAttachment(task);
                attachments.add(att);
                initConn(att);
            }

            while (!Thread.interrupted()) {
                final int selected = selector.select(1000);
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
                                }
                            }
                        } catch (final IOException x) {
                            LOG.error(String.format("SELECTED KEY %s HANDLING ERROR", getRequestUrl(key)), x);
                            onError(key, x);
                        }
                    }
                } else {
                    //                        waitABit(5000L + ts - System.currentTimeMillis());
                    selector.keys().forEach(key -> {
                        if (key.channel().isOpen()
                                && ((SocketChannel) key.channel()).isConnected()
                                && 0 == key.interestOps()) {
                            final Att attachment = (Att) key.attachment();
                            final long elapsedMs = System.currentTimeMillis() - attachment.ts;
                            if (elapsedMs > REQ_PERIOD) {
                                LOG.debug("Time to request {} after idle timeout: {}",
                                        attachment.task.url, elapsedMs);
                                attachment.ts = System.currentTimeMillis();
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                key.selector().wakeup();
                            }
                        }
                    });
                }
                if (selector.keys().size() != requests.size()) {
                    attachments.forEach(req -> {
                        if (selector.keys().stream().noneMatch(sk -> {
                            final Att a = (Att) sk.attachment();
                            return req.address.equals(a.address);
                        })) {
                            try {
                                final long elapsedMs = System.currentTimeMillis() - req.ts;
                                if (elapsedMs >= REQ_PERIOD) {
                                    LOG.debug("Time to connect {} after error timeout: {}",
                                            req.task.url, elapsedMs);
                                    initConn(req);
                                }
                            } catch (final IOException e) {
                                LOG.error(String.format("ERROR CONNECTING %s AFTER ERROR", req.task.url), e);
                            }
                        }
                    });
                }
            }
        } catch (final IOException x) {
            LOG.error("ERROR", x);
        } finally {
            close();
        }
    }

    private URL getRequestUrl(final SelectionKey key) {
        return ((Att) key.attachment()).task.url;
    }

    /**
     * Debug usage only
     *
     * @param timeout time to wait
     */
    @SuppressWarnings("unused")
    private void waitABit(final long timeout) {
        try {
            synchronized (objLock) {
                TimeUnit.MILLISECONDS.timedWait(objLock, timeout);
            }
        } catch (final InterruptedException e) {
            LOG.error("INTERRUPTED", e);
        }
    }

    private void write(final SelectionKey key) throws IOException {
        final Att attachment = (Att) key.attachment();
        if (attachment.writeCompleted()) {
            return;
        }

        // reset read state
        attachment.response.setLength(0); // attachment.sb.delete(0, attachment.sb.length());
        attachment.received = 0;

        final SocketChannel channel = (SocketChannel) key.channel();
        final ByteBuffer buffer = attachment.msgBuffer;

        final int written = channel.write(buffer);
        attachment.written += written;

        LOG.debug("addr: {} WRITTEN {} bytes ({} {})",
                channel.getRemoteAddress(),
                attachment.written,
                attachment.written == buffer.limit() ? "all of" : "less than",
                buffer.limit());
        // lets get ready to read.
        if (attachment.writeCompleted()) {
            attachment.written = 0;
            buffer.rewind();
            key.interestOps(
                    (key.interestOps() & (~SelectionKey.OP_WRITE)) | SelectionKey.OP_READ);
            key.selector().wakeup();
        }
    }

    public static final class Att {
        final SocketAddress address;
        final ByteBuffer msgBuffer;
        final ByteBuffer buffer;
        final StringBuilder response;
        final BasicTask task;
        int contentLength;
        boolean chunked;
        int headerLength;
        int received;
        long ts;
        int written;
        Throwable error;

        Att(final BasicTask task, final SocketAddress address,
            final ByteBuffer aMsgBuffer,
            final ByteBuffer buffer,
            final StringBuilder stringBuilder) {
            this.task = task;
            this.msgBuffer = aMsgBuffer;
            this.buffer = buffer;
            this.address = address;
            response = stringBuilder;
        }

        public boolean readCompleted() {
            return 0 < response.length() && ('}' == response.charAt(response.length() - 1));
        }

        public void reset() {
            written = 0;
            received = 0;
            error = null;
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
            return Optional.ofNullable(response).map(s -> s.toString().substring(headerLength))
                    .orElse("");
        }

    }
}
