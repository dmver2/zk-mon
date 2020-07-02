package home.dv.zkmon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
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
    private static final Logger LOG = LoggerFactory.getLogger(NioController.class);
    private final Object objLock = new Object();
    private final List<BasicTask> requests;
    private final NetStreamHandler handler = new HttpStreamHandler();
    private List<Att> attachments;
    private Selector selector;

    NioController(List<BasicTask> requests) {
        this.requests = requests;
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            LOG.trace("Application is about to close {}...", closeable);
            closeable.close();
        } catch (IOException e) {
            LOG.error("FAILED CLOSING {}", closeable, e);
        }
    }

    @Override
    public void close() {
        closeQuietly(selector);
    }

    private void initConn(Att attachment) throws IOException {
        SocketChannel channel;
        channel = SocketChannel.open();
        var rcvBufSize = channel.getOption(StandardSocketOptions.SO_RCVBUF);
        LOG.debug("recv buffer size: {} {} {}", rcvBufSize, (rcvBufSize < RECV_BUF) ? '<' : ">=",
                RECV_BUF);
        if (rcvBufSize < RECV_BUF) {
            channel.setOption(StandardSocketOptions.SO_RCVBUF, RECV_BUF);
            LOG.debug("new recv buffer size set to {} instead of {}",
                    channel.getOption(StandardSocketOptions.SO_RCVBUF), rcvBufSize);
        }
        channel.configureBlocking(false);
        channel.register(this.selector,
                SelectionKey.OP_CONNECT, attachment);
        channel.connect(attachment.address);
    }

    private Att makeAttachment(BasicTask task) {
        byte[] messageBytes =
                task.request.getBytes(StandardCharsets.ISO_8859_1);

        return new Att(
                task,
                task.address,
                ByteBuffer.wrap(messageBytes),
                ByteBuffer.allocate(0x400),
                new StringBuilder(0x800)
        );
    }

    private void onConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
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

    private void onError(SelectionKey key, Exception x) {
        key.cancel();
        Optional.ofNullable(key.channel())
                .filter(AbstractInterruptibleChannel::isOpen)
                .ifPresent(NioController::closeQuietly);
        Att atta = (Att) key.attachment();
        atta.error = x;
        atta.task.accept(atta);
        atta.reset();
        atta.ts = System.currentTimeMillis();
        //            waitABit();
        try {
            initConn(atta);
        } catch (IOException e) {
            LOG.error("ERROR CONNECTING AFTER ERROR", e);
        }
    }

    private void read(SelectionKey key) throws IOException {
        Att attachment = (Att) key.attachment();
        if (attachment.readCompleted()) {
            return;
        }
        // reset write state
        attachment.written = 0;

        SocketChannel channel = (SocketChannel) key.channel();

        ByteBuffer readBuffer = attachment.buffer; // ByteBuffer.allocate(0x400);
        readBuffer.clear();
        int iobytes;
        try {
            iobytes = channel.read(readBuffer);
        } catch (IOException e) {
            LOG.error("Reading problem, closing connection to {}",
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
            //TODO: task.run();
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
            for (BasicTask task : requests) {
                Att att = makeAttachment(task);
                attachments.add(att);
                initConn(att);
            }

            while (!Thread.interrupted()) {
                long ts = System.currentTimeMillis();
                int selected = selector.select(1000);
                LOG.debug("selected: {}", selected);
                if (0 < selected) {
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
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
                        } catch (IOException x) {
                            LOG.error("SELECTED KEY HANDLING ERROR", x);
                            onError(key, x);
                        }
                    }
                } else {
                    //                        waitABit(5000L + ts - System.currentTimeMillis());
                    selector.keys().forEach(key -> {
                        if (key.channel().isOpen()
                                && ((SocketChannel) key.channel()).isConnected()
                                && 0 == key.interestOps()) {
                            Att attachment = (Att) key.attachment();
                            if (System.currentTimeMillis() - attachment.ts > 5000L) {
                                LOG.debug("Time to request after idle timeout: {}",
                                        System.currentTimeMillis() - attachment.ts);
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
                            Att a = (Att) sk.attachment();
                            return req.address.equals(a.address);
                        })) {
                            try {
                                if (System.currentTimeMillis() - req.ts >= 5000L) {
                                    LOG.debug("Time to connect after error timeout: {}",
                                            System.currentTimeMillis() - req.ts);
                                    initConn(req);
                                }
                            } catch (IOException e) {
                                LOG.error("ERROR CONNECTING AFTER ERROR", e);
                            }
                        }
                    });
                }
            }
        } catch (IOException x) {
            LOG.error("ERROR", x);
        } finally {
            close();
        }
    }

    private void waitABit(long timeout) {
        try {
            synchronized (objLock) {
                TimeUnit.MILLISECONDS.timedWait(objLock, timeout);
            }
        } catch (InterruptedException e) {
            LOG.error("INTERRUPTED", e);
        }
    }

    private void write(SelectionKey key) throws IOException {
        Att attachment = (Att) key.attachment();
        if (attachment.writeCompleted()) {
            return;
        }

        // reset read state
        attachment.response.setLength(0); // attachment.sb.delete(0, attachment.sb.length());
        attachment.received = 0;

        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = attachment.msgBuffer;

        int written = channel.write(buffer);
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

        Att(final BasicTask task, SocketAddress address,
            ByteBuffer aMsgBuffer,
            ByteBuffer buffer,
            StringBuilder stringBuilder) {
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
            StringBuilder sb =
                    new StringBuilder(
                            0x200 + Optional.ofNullable(this.response).map(e -> e.length())
                                    .orElse(0));
            sb.append("Att{");
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
