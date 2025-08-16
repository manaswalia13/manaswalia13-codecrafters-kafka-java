import common.ApiKeyEnum;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");
        nio();
    }

    private static void nio() {
        final int port = 9092;
        try {
            ServerSocketChannel server = ServerSocketChannel.open();
            server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            server.bind(new InetSocketAddress(port));
            server.configureBlocking(false);

            Selector selector = Selector.open();
            server.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select(500);
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isAcceptable()) {
                        SocketChannel client = server.accept();
                        client.configureBlocking(false);
                        client.register(selector, SelectionKey.OP_READ);
                    }
                    else if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(2048);
                        int len = channel.read(buffer);

                        if (len > 0) {
                            buffer.flip();

                            // --- Decode request ---
                            buffer.getInt(); // length
                            int apiKey = buffer.getShort();
                            int apiVersion = buffer.getShort();
                            int correlationId = buffer.getInt();

                            List<Byte> data = new ArrayList<>();
                            fillBytes(data, (byte)0, 4);        // length placeholder
                            fillBytes(data, correlationId, 4);  // correlationId

                            if (apiKey == 18) { // ApiVersions
                                if (apiVersion < ApiKeyEnum.API_18.minVersion ||
                                    apiVersion > ApiKeyEnum.API_18.maxVersion) {
                                    fillBytes(data, (short)35, 2); // error code
                                } else {
                                    fillBytes(data, (short)0, 2); // no error
                                }

                                fillBytes(data, ApiKeyEnum.values().length, 1); // num keys
                                for (ApiKeyEnum api : ApiKeyEnum.values()) {
                                    fillBytes(data, (short) api.key, 2);
                                    fillBytes(data, (short) api.minVersion, 2);
                                    fillBytes(data, (short) api.maxVersion, 2);
                                    fillBytes(data, (byte)0, 1); // tagged fields
                                }
                            } else {
                                fillBytes(data, (short)0, 2);
                            }

                            ByteBuffer writeBuffer = ByteBuffer.allocate(data.size());
                            for (Byte b : data) writeBuffer.put(b);
                            writeBuffer.putInt(0, data.size() - 4);
                            writeBuffer.flip();

                            while (writeBuffer.hasRemaining()) {
                                channel.write(writeBuffer);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void fillBytes(List<Byte> data, byte value, int len) {
        for (int i = 0; i < len; i++) {
            data.add(value);
        }
    }

    private static void fillBytes(List<Byte> data, int value, int len) {
        ByteBuffer buf = ByteBuffer.allocate(4).putInt(value);
        byte[] arr = buf.array();
        for (int i = 4 - len; i < 4; i++) {
            data.add(arr[i]);
        }
    }

    private static void fillBytes(List<Byte> data, short value, int len) {
        ByteBuffer buf = ByteBuffer.allocate(2).putShort(value);
        byte[] arr = buf.array();
        for (int i = 2 - len; i < 2; i++) {
            data.add(arr[i]);
        }
    }
}
