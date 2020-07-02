package home.dv.zkmon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpStreamHandler implements NetStreamHandler {
    private static final Pattern CHUNKED_TRANSFER_ENC_PTN = Pattern
            .compile("^Transfer-Encoding:\\s+chunked$", Pattern.DOTALL | Pattern.MULTILINE);
    private static final int CHUNKED_TRANSFER_ENC_LENGTH = CHUNKED_TRANSFER_ENC_PTN.pattern()
            .length();
    private static final Pattern CONTENT_LENGTH_PATTERN = Pattern
            .compile("^Content-Length:\\s+(\\d+)$", Pattern.DOTALL | Pattern.MULTILINE);
    private static final int CONTENT_LENGTH_PATTERN_LENGTH = CONTENT_LENGTH_PATTERN.pattern()
            .indexOf(':') + 16;
    private static final String HDR_MARK_STR = "\r\n\r\n";
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpStreamHandler.class);

    @Override
    public boolean readBytes(final Integer ioBytes, final NioController.Att actionInfo) {
        actionInfo.received += ioBytes;
        final ByteBuffer buffer = actionInfo.buffer;
        buffer.flip();
        final String chunk = StandardCharsets.ISO_8859_1.decode(buffer).toString();
        LOGGER.trace(">> " + chunk);

        final StringBuilder sb = actionInfo.response;
        final int searchPosition = sb.length();
        sb.append(chunk);
        final String responseAsStr = sb.toString();

        if (0 == actionInfo.contentLength) {
            final Matcher m = CONTENT_LENGTH_PATTERN.matcher(responseAsStr);
            if (m.find(Math.max(0, searchPosition - CONTENT_LENGTH_PATTERN_LENGTH))) {
                actionInfo.contentLength = Integer.parseInt(m.group(1));
            }
        } else {
            if (CHUNKED_TRANSFER_ENC_PTN.matcher(responseAsStr)
                    .find(Math.max(0, searchPosition - CHUNKED_TRANSFER_ENC_LENGTH))) {
                actionInfo.chunked = true;
            }
        }
        // find end of header
        if (0 == actionInfo.headerLength) {
            final int eohPos;
            if (-1 != (eohPos = responseAsStr.indexOf(HDR_MARK_STR,
                    searchPosition - HDR_MARK_STR.length()))) {
                actionInfo.headerLength = eohPos + HDR_MARK_STR.length();
            }
        }
        final int headerLength = actionInfo.headerLength;
        final int contentLength = actionInfo.contentLength;

        boolean eof = false;
        if (!(0 == contentLength || 0 == headerLength)) {
            if (contentLength <= sb.length() - headerLength) {
                LOGGER.trace("EOF>>>");
                eof = true;
            }
        } else if (actionInfo.chunked) {
            LOGGER.trace("[DETECTED] chunkedTransferEncoding");
            if (chunk.endsWith("0\r\n")) {
                eof = true;
            }
        }
        buffer.clear();
        return eof;
    }
}
