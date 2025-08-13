import common.ApiKeyEnum;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    /*ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    try {
        serverSocket = new ServerSocket(port);
        // Since the tester restarts your program quite often, setting
    SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        serverSocket.setReuseAddress(true);
        // Wait for connection from client.
        clientSocket = serverSocket.accept();
    } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
    } finally {
        try {
            if (clientSocket != null) {
                clientSocket.close();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }*/
    nio();
  }
  private static void nio() {
    final int port = 9092;
    try {
      ServerSocketChannel server =
          ServerSocketChannel.open()
              .setOption(StandardSocketOptions.SO_REUSEADDR, true)
              //.setOption(StandardSocketOptions.SO_REUSEPORT, true)
              .setOption(StandardSocketOptions.SO_RCVBUF, 1024)
              .bind(new InetSocketAddress(port));
      server.configureBlocking(false);
      Selector selector = Selector.open();
      server.register(selector, SelectionKey.OP_ACCEPT);
      while (true) {
        if (selector.select(500) > 0) {
          Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
          while (iterator.hasNext()) {
            SelectionKey selectionKey = iterator.next();
            switch (selectionKey.interestOps()) {
            case SelectionKey.OP_ACCEPT: {
              SocketChannel clientChannel =
                  ((ServerSocketChannel)selectionKey.channel()).accept();
              clientChannel.configureBlocking(false);
              clientChannel.register(selector, SelectionKey.OP_READ);
            } break;
            case SelectionKey.OP_READ: {
              SocketChannel channel = (SocketChannel)selectionKey.channel();
              if (channel.isConnected()) {
                ByteBuffer buffer = ByteBuffer.allocate(2048);
                buffer.clear();
                int len = channel.read(buffer);
                if (len > 0) {
                  final List<Byte> data = new ArrayList<>();
                  // message length
                  fillBytes(data, (byte)0, 4);
                  // correlationId
                  fillBytes(data, buffer, 8, 4);

                  byte[] apiKey = new byte[2];
                  // apiKey
                  buffer.position(4);
                  buffer.get(apiKey, 0, 2);
                  BigInteger apiKeyNo = new BigInteger(apiKey);
                  switch (apiKeyNo.intValue()) {
                  case 18: {
                    // ApiVersions
                    buffer.position(6);
                    byte[] apiVers = new byte[2];
                    buffer.get(apiVers, 0, 2);
                    byte apiVersion = new BigInteger(apiVers).byteValue();
                    if (apiVersion < ApiKeyEnum.API_18.minVersion ||
                        apiVersion > ApiKeyEnum.API_18.maxVersion) {
                      fillBytes(data, 35, 2);
                    } else {
                      fillBytes(data, (byte)0, 2);
                    }
                    fillBytes(data, ApiKeyEnum.values().length + 1, 1);
                    Arrays.stream(ApiKeyEnum.values()).forEach(api -> {
                      fillBytes(data, api.key, 2);
                      fillBytes(data, api.minVersion, 2);
                      fillBytes(data, api.maxVersion, 2);
                      // tag fields
                      fillBytes(data, 0, 1);
                    });
                    if (apiVersion > 0) {
                      // fillBytes(data, (byte)0, 4);
                      fillBytes(data, 1, 4);
                      // tag fields
                      fillBytes(data, 0, 1);
                    }
                  } break;
                  default: {
                    // error code
                    fillBytes(data, (byte)0, 2);
                  }
                  }
                  ByteBuffer writeBuffer = ByteBuffer.allocate(data.size());
                  writeBuffer.position(0);
                  data.forEach(writeBuffer::put);
                  writeBuffer.putInt(0, data.size() - 4);
                  /*writeBuffer.position(0);
                  for(int i = 0; i < writeBuffer.capacity(); i++) {
                      System.out.printf("%02X ", writeBuffer.get());
                  }*/
                  writeBuffer.position(0);
                  while (writeBuffer.hasRemaining()) {
                    channel.write(writeBuffer);
                  }
                }
                // channel.close();
              }
            } break;
            case SelectionKey.OP_WRITE: {
            } break;
            default: {
            }
            }
            iterator.remove();
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
    buffer.position(initPosition);
    for (int i = 0; i < len; i++) {
      data.add(buffer.get());
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
        data.add((byte)0);
      }
      for (byte val : bytes) {
        data.add(val);
      }
    }
  }
}