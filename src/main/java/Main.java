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

        int port = Integer.parseInt(System.getenv().getOrDefault("KAFKA_PORT", "9092"));

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) { // Accept multiple clients (serial handling)
                try (Socket clientSocket = serverSocket.accept()) {
                    InputStream in = clientSocket.getInputStream();
                    OutputStream out = clientSocket.getOutputStream();

                    while (true) { // Handle multiple requests from same client
                        byte[] sizeBytes = in.readNBytes(4);
                        if (sizeBytes.length < 4) break; // Client closed
                        int size = ByteBuffer.wrap(sizeBytes).getInt();

                        byte[] body = in.readNBytes(size);
                        if (body.length < size) break; // Incomplete

                        short apiKey = ByteBuffer.wrap(body, 0, 2).getShort();
                        short apiVersion = ByteBuffer.wrap(body, 2, 2).getShort();
                        int correlationId = ByteBuffer.wrap(body, 4, 4).getInt();

                        System.out.println("ApiKey=" + apiKey + ", Version=" + apiVersion + ", CorrId=" + correlationId);

                        short errorCode = (apiKey != API_VERSIONS_KEY || apiVersion > MAX_SUPPORTED_VERSION)
                                ? (short) 35
                                : (short) 0;

                        // Build ApiVersions response
                        ByteBuffer responseBuffer = ByteBuffer.allocate(256);
                        responseBuffer.putInt(0); // placeholder for size
                        responseBuffer.putInt(correlationId);
                        responseBuffer.putShort(errorCode);

                        // ApiVersions array (1 element)
                        responseBuffer.put((byte) 1); // array length
                        responseBuffer.putShort(API_VERSIONS_KEY);
                        responseBuffer.putShort((short) 0); // min version
                        responseBuffer.putShort(MAX_SUPPORTED_VERSION); // max version

                        responseBuffer.put((byte) 0); // tagged fields

                        // Fill in the size
                        int messageSize = responseBuffer.position() - 4;
                        responseBuffer.putInt(0, messageSize);

                        byte[] response = new byte[responseBuffer.position()];
                        responseBuffer.flip();
                        responseBuffer.get(response);

                        out.write(response);
                        out.flush();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
