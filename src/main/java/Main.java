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
      // Open channel, set options separately to keep type
      ServerSocketChannel server = ServerSocketChannel.open();
      server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
      // server.setOption(StandardSocketOptions.SO_REUSEPORT, true); // optional
      server.setOption(StandardSocketOptions.SO_RCVBUF, 1024);
      server.bind(new InetSocketAddress(port));
      server.configureBlocking(false);

      Selector selector = Selector.open();
      server.register(selector, SelectionKey.OP_ACCEPT);

      while (true) {
        if (selector.select(500) > 0) {
          Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
          while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove(); // remove early to avoid reprocessing

            try {
              if (key.isAcceptable()) {
                SocketChannel clientChannel = ((ServerSocketChannel) key.channel()).accept();
                if (clientChannel != null) {
                  clientChannel.configureBlocking(false);
                  clientChannel.register(selector, SelectionKey.OP_READ);
                }
              } else if (key.isReadable()) {
                SocketChannel channel = (SocketChannel) key.channel();
                if (channel.isConnected()) {
                  ByteBuffer buffer = ByteBuffer.allocate(2048);
                  int len = channel.read(buffer);
                  if (len > 0) {
                    final List<Byte> data = new ArrayList<>();
                    buffer.flip();

                    // message length placeholder
                    fillBytes(data, (byte) 0, 4);
                    // correlationId
                    fillBytes(data, buffer, 8, 4);

                    // apiKey & apiVersion
                    int apiKey = (buffer.limit() >= 6) ? (buffer.getShort(4) & 0xFFFF) : -1;
                    int apiVersion = (buffer.limit() >= 8) ? (buffer.getShort(6) & 0xFFFF) : 0;

                    if (apiKey == 18) {
                      if (apiVersion < ApiKeyEnum.API_18.minVersion ||
                          apiVersion > ApiKeyEnum.API_18.maxVersion) {
                        fillBytes(data, 35, 2);
                      } else {
                        fillBytes(data, (byte) 0, 2);
                      }
                      fillBytes(data, ApiKeyEnum.values().length + 1, 1);
                      Arrays.stream(ApiKeyEnum.values()).forEach(api -> {
                        fillBytes(data, api.key, 2);
                        fillBytes(data, api.minVersion, 2);
                        fillBytes(data, api.maxVersion, 2);
                        fillBytes(data, 0, 1); // tag fields
                      });
                      if (apiVersion > 0) {
                        fillBytes(data, 1, 4);
                        fillBytes(data, 0, 1); // tag fields
                      }
                    } else {
                      fillBytes(data, (byte) 0, 2); // generic success
                    }

                    // write back
                    ByteBuffer writeBuffer = ByteBuffer.allocate(data.size());
                    data.forEach(writeBuffer::put);
                    writeBuffer.putInt(0, data.size() - 4);
                    writeBuffer.flip();
                    while (writeBuffer.hasRemaining()) {
                      channel.write(writeBuffer);
                    }
                  }
                }
              }
            } catch (IOException e) {
              key.cancel();
              try { key.channel().close(); } catch (IOException ignore) {}
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.printf("Exception: %s\n", e.getMessage());
    }
  }

  private static void fillBytes(List<Byte> data, byte value, int len) {
    for (int i = 0; i < len; i++) {
      data.add(value);
    }
  }

  private static void fillBytes(List<Byte> data, ByteBuffer buffer,
                                int initPosition, int len) {
    if (buffer.limit() >= initPosition + len) {
      for (int i = initPosition; i < initPosition + len; i++) {
        data.add(buffer.get(i));
      }
    } else {
      for (int i = 0; i < len; i++) {
        data.add((byte) 0);
      }
    }
  }

  private static void fillBytes(List<Byte> data, int value, int len) {
    byte[] bytes = BigInteger.valueOf(value).toByteArray();
    if (bytes.length >= len) {
      for (int i = bytes.length - len; i < bytes.length; i++) {
        data.add(bytes[i]);
      }
    } else {
      for (int i = 0, c = len - bytes.length; i < c; i++) {
        data.add((byte) 0);
      }
      for (byte val : bytes) {
        data.add(val);
      }
    }
  }
}
