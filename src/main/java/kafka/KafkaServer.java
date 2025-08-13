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
                if (sizeBytes.length < 4) break;

                int requestSize = ByteBuffer.wrap(sizeBytes).getInt();
                if (requestSize <= 0) break;

                byte[] requestBytes = input.readNBytes(requestSize);
                if (requestBytes.length < requestSize) break;

                byte[] response = processRequest(requestBytes);

                // Write frame length + response
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
         * ApiVersionsResponse v3/v4 format:
         * int32 correlationId
         * int16 errorCode
         * array[ApiKeys]
         * int32 throttle_time_ms
         * byte tagged_fields
         * Total = 18 bytes for one ApiKey entry
         */
        ByteBuffer body = ByteBuffer.allocate(18);
        body.putInt(correlationId);
        body.putShort(errorCode);

        body.put((byte) 1);            // Array length = 1
        body.putShort((short) 18);     // ApiKey
        body.putShort((short) 0);      // MinVersion
        body.putShort((short) 4);      // MaxVersion

        body.putInt(0);                // throttle_time_ms = 0
        body.put((byte) 0);            // tagged_fields = 0

        // Prevent extra/trailing bytes
        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }

    private byte[] buildErrorResponse(int correlationId) {
        // Minimal error frame for unknown API
        ByteBuffer body = ByteBuffer.allocate(7);
        body.putInt(correlationId);
        body.putShort((short) 44); // UNKNOWN_API_KEY
        body.put((byte) 0);        // No ApiVersions entries
        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }
}
