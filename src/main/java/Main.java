import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        int port = 9092;
        final short API_VERSIONS_KEY = 18;
        final short MAX_SUPPORTED_VERSION = 4;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            try (Socket clientSocket = serverSocket.accept()) {
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                while (true) {
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) break;

                    int requestSize = ByteBuffer.wrap(sizeBytes).getInt();
                    byte[] requestBytes = in.readNBytes(requestSize);
                    if (requestBytes.length < requestSize) break;

                    short apiKey = ByteBuffer.wrap(requestBytes, 0, 2).getShort();
                    short apiVersion = ByteBuffer.wrap(requestBytes, 2, 2).getShort();
                    int correlationId = ByteBuffer.wrap(requestBytes, 4, 4).getInt();

                    System.out.println("API Key: " + apiKey + " | Version: " + apiVersion + " | CorrelationId: " + correlationId);

                    // Determine error code
                    short errorCode = (apiKey != API_VERSIONS_KEY || apiVersion > MAX_SUPPORTED_VERSION)
                            ? (short) 35
                            : (short) 0;

                    // Send ApiVersionsResponse (size 19)
                    byte[] messageSize = ByteBuffer.allocate(4).putInt(19).array();
                    out.write(messageSize);
                    out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
                    out.write(ByteBuffer.allocate(2).putShort(errorCode).array());
                    out.write(new byte[] {2, 0x00, 0x12, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0});
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
