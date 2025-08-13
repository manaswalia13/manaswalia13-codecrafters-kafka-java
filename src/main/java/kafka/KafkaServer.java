import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class KafkaServer {
    private final int port;

    public KafkaServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start(); // handle multiple clients
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
            InputStream input = clientSocket.getInputStream();
            OutputStream output = clientSocket.getOutputStream()
        ) {
            while (true) {
                // Read 4-byte frame size
                byte[] sizeBytes = input.readNBytes(4);
                if (sizeBytes.length < 4) break;

                int requestSize = ByteBuffer.wrap(sizeBytes).getInt();
                if (requestSize <= 0) break;

                // Read request payload
                byte[] requestBytes = input.readNBytes(requestSize);
                if (requestBytes.length < requestSize) break;

                // Process & respond
                byte[] response = processRequest(requestBytes);

                // Send length + payload
                output.write(ByteBuffer.allocate(4).putInt(response.length).array());
                output.write(response);
                output.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

    private byte[] processRequest(byte[] requestBytes) {
        if (requestBytes.length < 8) return new byte[0];

        ByteBuffer buffer = ByteBuffer.wrap(requestBytes);

        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();

        if (apiKey == 18) { // ApiVersions request
            return buildApiVersionsResponse(correlationId, apiVersion);
        }

        return buildErrorResponse(correlationId);
    }

    private byte[] buildApiVersionsResponse(int correlationId, short requestApiVersion) {
        short errorCode = (requestApiVersion > 4) ? (short) 35 : (short) 0;

        /*
         * ApiVersionsResponse v3 format:
         * int32 correlationId
         * int16 errorCode
         * int32 array_length
         *   int16 apiKey
         *   int16 minVersion
         *   int16 maxVersion
         * int32 throttle_time_ms
         */
        ByteBuffer body = ByteBuffer.allocate(4 + 2 + 4 + 2 + 2 + 2 + 4);
        body.putInt(correlationId);
        body.putShort(errorCode);

        body.putInt(1);               // array length = 1 (int32)
        body.putShort((short) 18);    // ApiKey
        body.putShort((short) 0);     // MinVersion
        body.putShort((short) 4);     // MaxVersion

        body.putInt(0);               // throttle_time_ms = 0

        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }

    private byte[] buildErrorResponse(int correlationId) {
        // Minimal error frame for unknown API
        ByteBuffer body = ByteBuffer.allocate(4 + 2 + 4); // correlationId + errorCode + array length
        body.putInt(correlationId);
        body.putShort((short) 44); // UNKNOWN_API_KEY
        body.putInt(0);            // No ApiVersions entries
        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }
}
