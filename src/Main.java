import java.io.IOException;

public class Main {

    private static void daemonize() throws IOException {
        System.in.close();
        System.out.close();
    }

    private static void run() {
        while (true) {

        }
    }

    public static void main(String[] args) throws IOException {
        daemonize();
        run();
    }
}
