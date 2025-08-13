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

        ByteBuffer body = ByteBuffer.allocate(18);
        body.putInt(correlationId);       // 4 bytes
        body.putShort(errorCode);         // 2 bytes

        body.put((byte) 1);               // ApiVersions array length
        body.putShort((short) 18);        // ApiKey
        body.putShort((short) 0);         // MinVersion
        body.putShort((short) 4);         // MaxVersion

        body.putInt(0);                   // throttle_time_ms
        body.put((byte) 0);               // tagged fields byte

        body.flip();  // Prepare buffer for reading
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }

    private byte[] buildErrorResponse(int correlationId) {
        ByteBuffer body = ByteBuffer.allocate(7);
        body.putInt(correlationId);
        body.putShort((short) 44); // UNKNOWN_API_KEY error code
        body.put((byte) 0);        // empty ApiVersions array
        body.flip();
        byte[] responseBytes = new byte[body.remaining()];
        body.get(responseBytes);
        return responseBytes;
    }
}
