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
                // 1. Read request length (Kafka requests start with a 4-byte size prefix)
                int requestSize;
                try {
                    requestSize = in.readInt();
                } catch (IOException eof) {
                    // client closed connection
                    break;
                }

                byte[] requestBytes = new byte[requestSize];
                in.readFully(requestBytes);

                // 2. For now, always send back an ApiVersions response (dummy)
                byte[] responseBytes = buildApiVersionsResponse();

                out.writeInt(responseBytes.length);
                out.write(responseBytes);
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("Client disconnected.");
        }
    }

    // TODO: Replace with real ApiVersions response for later stages
    private static byte[] buildApiVersionsResponse() {
        // Placeholder minimal response (empty)
        return new byte[0];
    }
}
