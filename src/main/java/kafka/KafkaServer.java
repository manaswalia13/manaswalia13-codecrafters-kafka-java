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
                // Handle each client sequentially (can be changed to threads if needed)
                handleClient(clientSocket);
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
            InputStream input = clientSocket.getInputStream();
            OutputStream output = clientSocket.getOutputStream()
        ) {
            while (true) {
                byte[] sizeBytes = input.readNBytes(4);
                if (sizeBytes.length < 4) break; // Client closed

                int requestSize = ByteBuffer.wrap(sizeBytes).getInt();
                if (requestSize <= 0) break;

                byte[] requestBytes = input.readNBytes(requestSize);
                if (requestBytes.length < requestSize) break; // Incomplete request

                byte[] response = processRequest(requestBytes);

                // Write length prefix + body
                output.write(ByteBuffer.allocate(4).putInt(response.length).array());
                if (response.length > 0) output.write(response);
                output.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

    private byte[] processRequest(byte[] requestBytes) {
        if (requestBytes.length < 8) return new byte[0]; // Not enough header data

        ByteBuffer buffer = ByteBuffer.wrap(requestBytes);

        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();

        if (apiKey == 18) { // ApiVersions request
            return buildApiVersionsResponse(correlationId, apiVersion);
        }

        // Unknown API
        return buildErrorResponse(correlationId);
    }

    private byte[] buildApiVersionsResponse(int correlationId, short requestApiVersion) {
        short errorCode = (requestApiVersion > 4) ? (short) 35 : (short) 0;

        /*
         * Kafka ApiVersionsResponse v3/v4 format:
         * int32  correlationId
         * int16  errorCode
         * array[ApiKeys]:
         *    int16 apiKey
         *    int16 minVersion
         *    int16 maxVersion
         * int32  throttle_time_ms
         * byte   tagged_fields (0 = none)
         *
         * Length = 4 + 2 + 1 + (2+2+2) + 4 + 1 = 18 bytes
         */
        ByteBuffer body = ByteBuffer.allocate(18);
        body.putInt(correlationId);     // 4 bytes
        body.putShort(errorCode);       // 2 bytes

        body.put((byte) 1);             // Array length = 1
        body.putShort((short) 18);      // ApiKey
        body.putShort((short) 0);       // MinVersion
        body.putShort((short) 4);       // MaxVersion

        body.putInt(0);                 // throttle_time_ms = 0 (no throttling)
        body.put((byte) 0);              // tagged_fields = 0

        return body.array();
    }

    private byte[] buildErrorResponse(int correlationId) {
        /*
         * Minimal valid error response for unknown API Versions request — not strictly needed unless you
         * want protocol compliance for unknown requests.
         */
        ByteBuffer body = ByteBuffer.allocate(7);
        body.putInt(correlationId);
        body.putShort((short) 44); // UNKNOWN_API_KEY
        body.put((byte) 0);        // No ApiVersions entries
        return body.array();
    }
}
