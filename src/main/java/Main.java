import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            try (Socket clientSocket = serverSocket.accept()) {
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                while (true) {
                    // Read 4 bytes for request size
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) {
                        break; // client disconnected
                    }
                    int requestSize = ByteBuffer.wrap(sizeBytes).getInt();

                    // Read the request body
                    byte[] requestBytes = in.readNBytes(requestSize);
                    if (requestBytes.length < requestSize) {
                        break; // client disconnected mid-request
                    }

                    // Parse API key and version
                    short apiKey = ByteBuffer.wrap(requestBytes, 0, 2).getShort();
                    short apiVersion = ByteBuffer.wrap(requestBytes, 2, 2).getShort();

                    // Parse correlation ID (starts at byte 4 now)
                    int correlationId = ByteBuffer.wrap(requestBytes, 4, 4).getInt();

                    System.out.println("CorrelationId: " + correlationId);

                    // Build ApiVersions response (length = 19 bytes)
                    byte[] messageSize = ByteBuffer.allocate(4).putInt(19).array();
                    out.write(messageSize);
                    out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
                    out.write(
                        apiKey != 18 || apiVersion > 4
                            ? new byte[] {0, 35} // unsupported version
                            : new byte[] {0, 0}  // supported
                    );
                    out.write(new byte[] {2, 0x00, 0x12, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0});
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
