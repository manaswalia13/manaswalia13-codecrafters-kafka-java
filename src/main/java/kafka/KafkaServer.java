package kafka;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.IntStream;

public class KafkaServer {
    private final int port;

    public KafkaServer(int port) {
        this.port = port;
    }

    public void start() {
        System.err.println("[INFO] Servidor Kafka iniciado na porta " + port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("[INFO] Aguardando conexão]");
            try (Socket clientSocket = serverSocket.accept()) {
                System.out.println("[INFO] Cliente conectado!");

                while (!clientSocket.isClosed()) {
                    try {
                        KafkaRequest request = new KafkaRequest(clientSocket.getInputStream());
                        byte[] response = KafkaResponseBuilder.buildResponse(request);
                        System.out.println("[INFO] Request recebida: \n" + request.toString());
                        System.out.println("[INFO] Bytes da resposta gerada: \n");
                        System.out.print("[HEX]");
                        IntStream.range(0, response.length)
                                .mapToObj(i -> String.format("%02X", response[i]))
                                .forEach(hex -> System.out.print(hex + " "));
                        System.out.println("");

                        clientSocket.getOutputStream().write(response);
                        clientSocket.getOutputStream().flush();

                    } catch (IOException e) {
                        System.out.println("[ERRO] Erro ao enviar response para a requisição");
                        System.out.println("[ERRO] " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.out.println("[ERRO] Erro ao aceitar conexão:");
                System.out.println("[ERRO] " + e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("[ERRO] Erro ao iniciar o servidor.");
            System.out.println("[ERRO] " + e.getMessage());
        }
    }
  }
