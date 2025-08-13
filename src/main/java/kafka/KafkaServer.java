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
                handleClient(clientSocket); // Serial Requests: handle same client until disconnect
            }
        }
    }

    private void handleClient(Socket clientSocket) throws IOException {
        try (InputStream input = clientSocket.getInputStream();
             OutputStream output = clientSocket.getOutputStream()) {

            while (true) {
                // Read request size (4 bytes)
                byte[] sizeBytes = input.readNBytes(4);
                if (sizeBytes.length < 4) {
                    break; // Client closed connection
                }

                int requestSize = ByteBuffer.wrap(sizeBytes).getInt();

                // Read request bytes
                byte[] requestBytes = input.readNBytes(requestSize);
                if (requestBytes.length < requestSize) {
                    break; // Incomplete request
                }

                // Process and get response
                byte[] response = processRequest(requestBytes);

                // Send size + response
                output.write(ByteBuffer.allocate(4).putInt(response.length).array());
                output.write(response);
                output.flush();
            }
        }
    }

    private byte[] processRequest(byte[] requestBytes) {
        ByteBuffer buffer = ByteBuffer.wrap(requestBytes);

        // Kafka request header: ApiKey (short), ApiVersion (short), CorrelationId (int)
        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();

        if (apiKey == 18) { // ApiVersions
            return buildApiVersionsResponse(correlationId, apiVersion);
        }

        // Unknown API → empty response
        return new byte[0];
    }

    private byte[] buildApiVersionsResponse(int correlationId, short requestApiVersion) {
        // Kafka ErrorCode 35 = UNSUPPORTED_VERSION
        short errorCode = (requestApiVersion > 4) ? (short) 35 : (short) 0;

        ByteBuffer body = ByteBuffer.allocate(256);

        // CorrelationId
        body.putInt(correlationId);

        // ErrorCode
        body.putShort(errorCode);

        // ApiVersions array length = 1
        body.put((byte) 1);

        // ApiKey = 18
        body.putShort((short) 18);

        // MinVersion = 0, MaxVersion = 4
        body.putShort((short) 0);
        body.putShort((short) 4);

        // Tag buffer (flexible versions) = 0
        body.put((byte) 0);

        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);

        return responseBytes;
    }
}
