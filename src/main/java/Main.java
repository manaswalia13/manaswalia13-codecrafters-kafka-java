import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        final short API_VERSIONS_KEY = 18;
        final short MAX_SUPPORTED_VERSION = 4;

        try (ServerSocket serverSocket = new ServerSocket(9092)) {
            serverSocket.setReuseAddress(true);
            try (Socket clientSocket = serverSocket.accept()) {
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                while (true) {
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) break; // client closed
                    int size = ByteBuffer.wrap(sizeBytes).getInt();

                    byte[] body = in.readNBytes(size);
                    if (body.length < size) break;

                    // parse fields: apiKey (2 bytes), apiVersion (2 bytes), correlationId (4 bytes)
                    short apiKey = ByteBuffer.wrap(body, 0, 2).getShort();
                    short apiVersion = ByteBuffer.wrap(body, 2, 2).getShort();
                    int correlationId = ByteBuffer.wrap(body, 4, 4).getInt();

                    System.out.println("ApiKey=" + apiKey + ", Version=" + apiVersion + ", CorrId=" + correlationId);

                    short errorCode = (apiKey != API_VERSIONS_KEY || apiVersion > MAX_SUPPORTED_VERSION)
                        ? (short) 35
                        : (short) 0;

                    // 15 bytes body: 4 (corrId) + 2 (err) + 9 (other bytes), total 19 including size
                    ByteBuffer buffer = ByteBuffer.allocate(19);
                    buffer.putInt(15); // total message size after this int
                    buffer.putInt(correlationId);
                    buffer.putShort(errorCode);
                    buffer.put(new byte[] {2, 0x00, 0x12, 0, 0, 0, 4, 0, 0}); // Ensure 9 bytes, not 12

                    byte[] response = buffer.array();

                    out.write(response);
                    out.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
