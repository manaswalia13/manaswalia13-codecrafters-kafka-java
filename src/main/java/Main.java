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

        try (ServerSocket serverSocket = new ServerSocket(9092)) {
            serverSocket.setReuseAddress(true);

            try (Socket clientSocket = serverSocket.accept()) {
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                while (true) {
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) break; // connection closed
                    int size = ByteBuffer.wrap(sizeBytes).getInt();

                    byte[] body = in.readNBytes(size);
                    if (body.length < size) break; // incomplete request

                    short apiKey = ByteBuffer.wrap(body, 0, 2).getShort();
                    short apiVersion = ByteBuffer.wrap(body, 2, 2).getShort();
                    int correlationId = ByteBuffer.wrap(body, 4, 4).getInt();

                    short errorCode = (apiKey != API_VERSIONS_KEY || apiVersion > MAX_SUPPORTED_VERSION)
                            ? (short) 35
                            : (short) 0;

                    // Build body (no size yet)
                    byte[] bodyResponse = ByteBuffer.allocate(15)
                            .putInt(correlationId)
                            .putShort(errorCode)
                            .put(new byte[]{2, 0x00, 0x12, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0})
                            .array();

                    // Now wrap with size prefix
                    out.write(ByteBuffer.allocate(4).putInt(bodyResponse.length).array());
                    out.write(bodyResponse);
                    out.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
