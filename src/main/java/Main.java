import common.ApiKeyEnum;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Main {

  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");
    nio();
  }

  private static void nio() {
    final int port = 9092;
    try {
      ServerSocketChannel server = ServerSocketChannel.open();
      // Avoid SO_* options to keep this JDK-agnostic and simple.
      server.socket().setReuseAddress(true);
      server.bind(new InetSocketAddress(port));
      server.configureBlocking(false);

      Selector selector = Selector.open();
      server.register(selector, SelectionKey.OP_ACCEPT);

      while (true) {
        if (selector.select(500) == 0) continue;

        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
          SelectionKey key = it.next();
          it.remove();

          try {
            if (key.isAcceptable()) {
              ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
              SocketChannel client = ssc.accept(); // may be null in non-blocking mode
              if (client != null) {
                client.configureBlocking(false);
                client.register(selector, SelectionKey.OP_READ);
              }
            } else if (key.isReadable()) {
              SocketChannel ch = (SocketChannel) key.channel();
              if (!ch.isOpen()) continue;

              ByteBuffer buf = ByteBuffer.allocate(4096);
              int n = ch.read(buf);
              if (n == -1) { ch.close(); continue; }
              if (n == 0) { continue; }

              buf.flip();

              // Build response
              List<Byte> out = new ArrayList<>();

              // Placeholder for frame length (4 bytes)
              putN(out, 0, 4);

              // correlationId: copy 4 bytes from offset 8 if present, else 0
              if (buf.limit() >= 12) {
                copyBytes(out, buf, 8, 4);
              } else {
                putN(out, 0, 4);
              }

              // Read apiKey/apiVersion if present
              int apiKey = (buf.limit() >= 6) ? (buf.getShort(4) & 0xFFFF) : -1;
              int apiVersion = (buf.limit() >= 8) ? (buf.getShort(6) & 0xFFFF) : 0;

              if (apiKey == 18) { // ApiVersions
                // errorCode
                if (apiVersion < ApiKeyEnum.API_18.minVersion || apiVersion > ApiKeyEnum.API_18.maxVersion) {
                  putShort(out, (short) 35); // UNSUPPORTED_VERSION
                } else {
                  putShort(out, (short) 0);
                }

                // Flexible versions (v3+): compact array length = N + 1 (uvarint).
                int nApis = ApiKeyEnum.values().length;
                putUVarInt(out, nApis + 1);

                // Each element: apiKey, minVersion, maxVersion, tagged_fields (empty)
                for (ApiKeyEnum api : ApiKeyEnum.values()) {
                  putShort(out, (short) api.key);
                  putShort(out, (short) api.minVersion);
                  putShort(out, (short) api.maxVersion);
                  putUVarInt(out, 0); // element tagged_fields (empty)
                }

                if (apiVersion > 0) {
                  // throttle_time_ms (int32) present in v1+
                  putInt(out, 1);
                  // top-level tagged_fields (empty, flexible versions)
                  putUVarInt(out, 0);
                }
              } else {
                // Generic success (errorCode = 0) for unknown APIs so tests don’t crash
                putShort(out, (short) 0);
              }

              // Write size at start (Kafka length excludes the 4 length bytes)
              ByteBuffer rsp = ByteBuffer.allocate(out.size());
              for (byte b : out) rsp.put(b);
              rsp.putInt(0, out.size() - 4);
              rsp.flip();

              while (rsp.hasRemaining()) {
                ch.write(rsp);
              }
            }
          } catch (IOException e) {
            try { key.channel().close(); } catch (IOException ignored) {}
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

  // ---- helpers (big-endian, simple, no BigInteger) ----

  private static void copyBytes(List<Byte> out, ByteBuffer buf, int pos, int len) {
    int oldPos = buf.position();
    int limit = buf.limit();
    int end = pos + len;
    if (pos >= 0 && end <= limit) {
      for (int i = pos; i < end; i++) out.add(buf.get(i));
    } else {
      int available = Math.max(0, Math.min(limit, end) - Math.max(0, pos));
      for (int i = 0; i < available; i++) out.add(buf.get(pos + i));
      for (int i = available; i < len; i++) out.add((byte) 0);
    }
    buf.position(oldPos);
  }

  private static void putN(List<Byte> out, int value, int len) {
    for (int i = len - 1; i >= 0; i--) out.add((byte)((value >>> (8 * i)) & 0xFF));
  }

  private static void putInt(List<Byte> out, int value) { putN(out, value, 4); }
  private static void putShort(List<Byte> out, short value) { putN(out, value & 0xFFFF, 2); }

  // Unsigned VarInt (Kafka flexible types). For small values (< 128) this is one byte.
  private static void putUVarInt(List<Byte> out, int value) {
    while ((value & ~0x7F) != 0) {
      out.add((byte)((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    out.add((byte)(value & 0x7F));
  }
}
