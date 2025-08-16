import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected!");

                handleClient(clientSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())
        ) {
            while (true) {
                int requestSize;

                try {
                    requestSize = in.readInt(); // 4-byte size prefix
                } catch (IOException eof) {
                    // client closed connection
                    break;
                }

                byte[] requestBytes = new byte[requestSize];
                in.readFully(requestBytes);

                // Parse correlation_id from request
                int correlationId = extractCorrelationId(requestBytes);

                // Build minimal ApiVersions response
                byte[] responseBytes = buildApiVersionsResponse(correlationId);

                // Send length-prefixed response
                out.writeInt(responseBytes.length);
                out.write(responseBytes);
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("Client disconnected.");
        }
    }

    private static int extractCorrelationId(byte[] requestBytes) throws IOException {
        try (DataInputStream reqIn = new DataInputStream(new ByteArrayInputStream(requestBytes))) {
            short apiKey = reqIn.readShort();      // 2 bytes
            short apiVersion = reqIn.readShort();  // 2 bytes
            int correlationId = reqIn.readInt();   // 4 bytes
            return correlationId;
        }
    }

    private static byte[] buildApiVersionsResponse(int correlationId) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);

        // === Response Header ===
        out.writeInt(correlationId); // correlation_id

        // === Response Body ===
        out.writeShort(0); // error_code = 0 (success)

        // api_versions array: empty (0 entries)
        out.writeInt(0);

        // throttle_time_ms
        out.writeInt(0);

        out.flush();
        return byteStream.toByteArray();
    }
}
