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
                // Handle each client in a new thread for concurrency, if desired
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
                if (sizeBytes.length < 4) break; // Client closed connection

                int requestSize = ByteBuffer.wrap(sizeBytes).getInt();
                if (requestSize <= 0) break; // Invalid size

                byte[] requestBytes = input.readNBytes(requestSize);
                if (requestBytes.length < requestSize) break; // Incomplete request

                byte[] response = processRequest(requestBytes);

                // Always reply with size + response, even if empty
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
        if (requestBytes.length < 8) return new byte[0]; // Not enough header

        ByteBuffer buffer = ByteBuffer.wrap(requestBytes);

        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();

        if (apiKey == 18) { // ApiVersions
            return buildApiVersionsResponse(correlationId, apiVersion);
        }

        // Unknown API: reply with error code and correlationId (optional per protocol, but less confusing to clients)
        return buildErrorResponse(correlationId);
    }

    private byte[] buildApiVersionsResponse(int correlationId, short requestApiVersion) {
        short errorCode = (requestApiVersion > 4) ? (short) 35 : (short) 0;

        // Calculate exact response body size: 4+2+1+2+2+2+1=14 bytes
        ByteBuffer body = ByteBuffer.allocate(14);
        body.putInt(correlationId);       // 4 bytes
        body.putShort(errorCode);         // 2 bytes
        body.put((byte) 1);               // 1 byte: ApiVersions array length
        body.putShort((short) 18);        // 2 bytes: ApiKey
        body.putShort((short) 0);         // 2 bytes: MinVersion
        body.putShort((short) 4);         // 2 bytes: MaxVersion
        body.put((byte) 0);               // 1 byte: tag buffer (flexible version marker)
        return body.array();
    }

    private byte[] buildErrorResponse(int correlationId) {
        // Typical error response: only correlationId, error code, and no versions advertised.
        ByteBuffer body = ByteBuffer.allocate(7);
        body.putInt(correlationId);       // 4 bytes
        body.putShort((short) 44);        // 2 bytes: 'unrecognized API key' (example; use correct code as needed)
        body.put((byte) 0);               // 1 byte: no ApiVersions
        return body.array();
    }
}
