import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            // Accept a single client (Serial Requests means one at a time)
            try (Socket clientSocket = serverSocket.accept()) {
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                while (true) {
                    // First read 4 bytes to get request size
                    byte[] sizeBytes = in.readNBytes(4);
                    if (sizeBytes.length < 4) {
                        break; // client disconnected
                    }

                    int requestSize = ByteBuffer.wrap(sizeBytes).getInt();

                    // Read the full request
                    byte[] requestBytes = in.readNBytes(requestSize);
                    if (requestBytes.length < requestSize) {
                        break; // client disconnected mid-request
                    }

                    // Extract ApiVersion and CorrelationId
                    byte[] apiVersion = new byte[2];
                    System.arraycopy(requestBytes, 6, apiVersion, 0, 2);

                    byte[] correlationId = new byte[4];
                    System.arraycopy(requestBytes, 8, correlationId, 0, 4);

                    System.out.println("CorrelationId: " + ByteBuffer.wrap(correlationId).getInt());

                    // Build response
                    var messageSize = ByteBuffer.allocate(4).putInt(19).array();
                    out.write(messageSize);
                    out.write(correlationId);
                    out.write(
                        apiVersion[0] != 0 || apiVersion[1] > 4
                            ? new byte[] {0, 35} // unsupported version
                            : new byte[] {0, 0}  // supported version
                    );
                    out.write(new byte[] {2, 00, 0x12, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0});
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
