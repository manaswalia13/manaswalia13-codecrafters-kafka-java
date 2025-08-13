public class Main {
    public static void main(String[] args) {
        int port = 9092; // Kafka default port
        KafkaServer server = new KafkaServer(port);

        try {
            System.out.println("Starting KafkaServer on port " + port + "...");
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
