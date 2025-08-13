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
                    // Read size prefix (4 bytes)
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) {
                        break; // client closed connection
                    }

                    int size = ByteBuffer.wrap(sizeBytes).getInt();

                    // Read request body
                    byte[] body = in.readNBytes(size);
                    if (body.length < size) {
                        break; // incomplete request
                    }

                    // Parse API key, version, correlation ID
                    short apiKey = ByteBuffer.wrap(body, 0, 2).getShort();
                    short apiVersion = ByteBuffer.wrap(body, 2, 2).getShort();
                    int correlationId = ByteBuffer.wrap(body, 4, 4).getInt();

                    // Decide error code
                    short errorCode;
                    if (apiKey == API_VERSIONS_KEY && apiVersion <= MAX_SUPPORTED_VERSION) {
                        errorCode = 0;
                    } else {
                        errorCode = 35;
                    }

                    // Build response body (correlationId + errorCode + fixed bytes)
                    byte[] bodyResponse = ByteBuffer.allocate(15)
                            .putInt(correlationId)
                            .putShort(errorCode)
                            .put(new byte[]{2, 0x00, 0x12, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0})
                            .array();

                    // Write size prefix + body
                    out.write(ByteBuffer.allocate(4).putInt(bodyResponse.length).array());
                    out.write(bodyResponse);
                    out.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
