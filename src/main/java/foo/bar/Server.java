package foo.bar;

import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Server {

  private static final Logger logger = LoggerFactory.getLogger(Server.class.getName());

  public static void main(String[] args) throws Exception {
    logger.info("Server startup. Args = {}", Arrays.toString(args));
    final Server fooBarServer = new Server();
    fooBarServer.start();
    fooBarServer.blockUntilShutdown();
  }

  static String host = "127.0.0.1";
  static int port = 42420;

  private io.grpc.Server server;

  private void start() throws Exception {
    logger.info("Starting the grpc server");

    server = ServerBuilder.forPort(port)
        .addService(new FooBarServiceImpl())
        .build()
        .start();

    logger.info("Server started. Listening on port {}", port);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.err.println("*** JVM is shutting down. Turning off grpc server as well ***");
      Server.this.stop();
      System.err.println("*** shutdown complete ***");
    }));
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

}
