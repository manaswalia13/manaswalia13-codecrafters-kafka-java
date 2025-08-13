import common.ApiKeyEnum;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Main {

  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");
    nio();
  }

  private static void nio() {
    final int port = 9092;
    try {
      // Open first, then set options individually (avoid chained type narrowing)
      ServerSocketChannel server = ServerSocketChannel.open();
      server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
      // server.setOption(StandardSocketOptions.SO_REUSEPORT, true); // optional, may not be supported everywhere
      server.setOption(StandardSocketOptions.SO_RCVBUF, 1024);
      server.bind(new InetSocketAddress(port));
      server.configureBlocking(false);

      Selector selector = Selector.open();
      server.register(selector, SelectionKey.OP_ACCEPT);

      while (true) {
        if (selector.select(500) == 0) {
          continue;
        }

        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();
          iterator.remove(); // remove first to avoid re-processing

          try {
            if (key.isAcceptable()) {
              ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
              SocketChannel clientChannel = ssc.accept(); // can be null in non-blocking mode
              if (clientChannel != null) {
                clientChannel.configureBlocking(false);
                clientChannel.register(selector, SelectionKey.OP_READ);
              }
            } else if (key.isReadable()) {
              SocketChannel channel = (SocketChannel) key.channel();
              if (!channel.isOpen()) continue;

              ByteBuffer buffer = ByteBuffer.allocate(2048);
              int len = channel.read(buffer);
              if (len == -1) {
                // client closed the connection
                channel.close();
                continue;
              }
              if (len == 0) {
                // no data this round
                continue;
              }

              buffer.flip(); // make Buffer ready for reading

              // Build response
              final List<Byte> data = new ArrayList<>();

              // message length placeholder (4 bytes)
              fillBytes(data, (byte) 0, 4);

              // correlationId: copy from request at offset 8 (4 bytes)
              if (buffer.limit() >= 12) {
                fillBytes(data, buffer, 8, 4);
              } else {
                // fallback: correlationId = 0
                fillBytes(data, 0, 4);
              }

              // Parse apiKey & apiVersion as UNSIGNED short (big-endian)
              if (buffer.limit() >= 8) {
                int apiKey = Short.toUnsignedInt(buffer.getShort(4));
                int apiVersion = (buffer.limit() >= 8) ? Short.toUnsignedInt(buffer.getShort(6)) : 0;

                switch (apiKey) {
                  case 18: { // ApiVersions
                    if (apiVersion < ApiKeyEnum.API_18.minVersion || apiVersion > ApiKeyEnum.API_18.maxVersion) {
                      // error: UNSUPPORTED_VERSION (35)
                      fillBytes(data, 35, 2);
                    } else {
                      // errorCode = 0
                      fillBytes(data, 0, 2);
                    }

                    // Number of API keys
                    fillBytes(data, ApiKeyEnum.values().length + 1, 1);

                    // List of (apiKey, minVersion, maxVersion, tagged-fields)
                    Arrays.stream(ApiKeyEnum.values()).forEach(api -> {
                      fillBytes(data, api.key, 2);
                      fillBytes(data, api.minVersion, 2);
                      fillBytes(data, api.maxVersion, 2);
                      // tagged fields
                      fillBytes(data, 0, 1);
                    });

                    if (apiVersion > 0) {
                      // Throttle time (int32) — choose 1ms
                      fillBytes(data, 1, 4);
                      // tagged fields
                      fillBytes(data, 0, 1);
                    }
                  } break;

                  default: {
                    // For other APIs: errorCode = 0 for now
                    fillBytes(data, 0, 2);
                  }
                }
              } else {
                // Not enough bytes to even read apiKey/apiVersion: respond with generic success (error=0)
                fillBytes(data, 0, 2);
              }

              // Write response length at the start
              ByteBuffer writeBuffer = ByteBuffer.allocate(data.size());
              for (byte b : data) writeBuffer.put(b);
              writeBuffer.putInt(0, data.size() - 4); // Kafka: first 4 bytes = size (not including itself)
              writeBuffer.flip();

              while (writeBuffer.hasRemaining()) {
                channel.write(writeBuffer);
              }
              // keep connection open (Kafka clients may pipeline)
            }
          } catch (IOException e) {
            // Close the channel on any IO problem to avoid busy loops
            try {
              key.channel().close();
            } catch (IOException ignored) {}
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.printf("Exception: %s%n", e.getMessage());
    }
  }

  private static void fillBytes(List<Byte> data, byte value, int len) {
    for (int i = 0; i < len; i++) {
      data.add(value);
    }
  }

  private static void fillBytes(List<Byte> data, ByteBuffer buffer, int initPosition, int len) {
    // Copy 'len' bytes from absolute position 'initPosition' if available
    int oldPos = buffer.position();
    int oldLimit = buffer.limit();
    int end = initPosition + len;
    if (end > oldLimit) {
      // not enough bytes; pad with zeros
      int available = Math.max(0, oldLimit - initPosition);
      if (available > 0) {
        buffer.position(initPosition);
        for (int i = 0; i < available; i++) data.add(buffer.get());
      }
      for (int i = available; i < len; i++) data.add((byte) 0);
    } else {
      buffer.position(initPosition);
      for (int i = 0; i < len; i++) data.add(buffer.get());
    }
    buffer.position(oldPos); // restore
  }

  private static void fillBytes(List<Byte> data, int value, int len) {
    // Write big-endian, fixed width
    for (int i = len - 1; i >= 0; i--) {
      data.add((byte) ((value >> (8 * i)) & 0xFF));
    }
  }
}
