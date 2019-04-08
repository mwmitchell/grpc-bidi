package foo.bar;

import com.google.common.base.Throwables;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Client {

  static final Logger logger = LoggerFactory.getLogger(Client.class.getName());

  private static final AtomicLong requestCount = new AtomicLong();
  private static final AtomicLong ioCount = new AtomicLong();
  private static final AtomicLong maxBlockingThreads = new AtomicLong();

  private static void spin(int milliseconds) {
    long sleepTime = milliseconds * 1000000L; // convert to nanoseconds
    long startTime = System.nanoTime();
    while ((System.nanoTime() - startTime) < sleepTime) {}
  }

  private static void doBlockingWork() {
    if (maxBlockingThreads.getAndIncrement() < 3) {
      if (ioCount.getAndIncrement() == 0) {
        logger.info("sleeper thread doing work...");
        //spin(10000);
        try {
          URL oracle = new URL("-->> url-of-a-very-large-file-here <<--");
          URLConnection yc = oracle.openConnection();
          try (BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()))) {
            String inputLine;
            int i;
            while ((inputLine = in.readLine()) != null) {
              //System.out.println(inputLine);
              i = 1;
            }
          }
        } catch (Exception e) {
          logger.info("http error!");
        }
      } else if (requestCount.getAndIncrement() == 1) {
        try {
          Thread.sleep(5_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } else if (requestCount.get() == 2) {
        spin(20_000);
      } else {
        requestCount.set(0);
      }
    } else {
      maxBlockingThreads.set(0);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    ManagedChannel ch = ManagedChannelBuilder
        .forAddress(Server.host, Server.port)
        .usePlaintext()
        //.executor(Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("ch-%d").build()))
        .build();

    int numConnectStreams = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numConnectStreams, new ThreadFactoryBuilder().setNameFormat("app-%d").build());

    foo.bar.FooBarServiceGrpc.FooBarServiceStub stub = foo.bar.FooBarServiceGrpc.newStub(ch).withExecutor(new ForkJoinPool(4));
    BlockingQueue<StreamObserver<foo.bar.ClientMessage>> streams = Queues.newArrayBlockingQueue(numConnectStreams);
    BlockingQueue<foo.bar.ServerMessage> serverMessages = Queues.newArrayBlockingQueue(100_000);
    BlockingQueue<foo.bar.ClientMessage> clientMessages = Queues.newArrayBlockingQueue(100_000);

    Runnable startWorkerTask = () -> {
      executor.submit(() -> {
        foo.bar.ServerMessage serverMessage;
        try {
          serverMessage = serverMessages.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        //
        // Uncomment the next line to do work in the application threads:
        //
        //doBlockingWork();
        try {
          StreamObserver<foo.bar.ClientMessage> requestStream = streams.take();
          foo.bar.ServerStreamRequest serverStreamRequest = serverMessage.getStreamRequest();
          String sid = UUID.randomUUID().toString();
          foo.bar.ClientMessage m = newStreamBeginMessage(sid);
          requestStream.onNext(m);
          streams.add(requestStream);
          //
          IntStream.range(0, serverStreamRequest.getNumItems()).forEach(i -> {
            StreamObserver<foo.bar.ClientMessage> s1;
            try {
              s1 = streams.take();
              s1.onNext(newStreamItem(sid));
              streams.add(s1);
              //requestStream.onNext(newStreamItem(sid));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          });
          StreamObserver<foo.bar.ClientMessage> s = streams.take();
          //requestStream.onNext(newStreamEndMessage(sid));
          s.onNext(newStreamEndMessage(sid));
          streams.add(s);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    };

    Consumer<foo.bar.ServerMessage> serverMessageConsumer = (msg) -> {
      try {
        serverMessages.put(msg);
        //logger.info("startingWorkerTask");
        startWorkerTask.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    CountDownLatch doneLatch = new CountDownLatch(numConnectStreams);

    IntStream.range(0, numConnectStreams).forEach(i -> {
      streams.add(establishConnectCall(i, stub, serverMessageConsumer, doneLatch));
    });


    doneLatch.await();
    System.exit(1);
  }

  private static StreamObserver<foo.bar.ClientMessage> establishConnectCall(
      Integer num, foo.bar.FooBarServiceGrpc.FooBarServiceStub stub, Consumer<foo.bar.ServerMessage> output, CountDownLatch doneLatch
  ) {

    logger.info("Establishing connect() call number {}", num);

    AtomicInteger clientNumber = new AtomicInteger(num);

    AtomicReference<StreamObserver<foo.bar.ClientMessage>> connectStreamRef = new AtomicReference<>();

    StreamObserver<foo.bar.ClientMessage> connectStream = stub.withWaitForReady().connect(new ClientResponseObserver<foo.bar.ClientMessage, foo.bar.ServerMessage>() {

      AtomicBoolean connected = new AtomicBoolean();
      AtomicReference<ClientCallStreamObserver<foo.bar.ClientMessage>> requestStreamRef = new AtomicReference<>();

      Runnable doConnect = () -> {
        requestStreamRef.get().onNext(foo.bar.ClientMessage.newBuilder()
            .setConnect(foo.bar.ClientConnect.newBuilder()
                .setCid(UUID.randomUUID().toString())).build());
      };

      @Override
      public void onNext(foo.bar.ServerMessage value) {
        ClientCallStreamObserver<foo.bar.ClientMessage> clientCallStream = requestStreamRef.get();
        switch (value.getMsgCase()) {
          case CONNECTREPLY:
            logger.info("Client {} - connect() succeeded; ServerMessage ConnectReply received: {}", clientNumber.get(), value.getMsgCase());
            clientCallStream.request(1);
            return;
          case STREAMREQUEST:
            //
            // Uncomment the next line to do work in the gRPC callback threads:
            // doBlockingWork();
            //
            logger.info("emitting value: {}", value);
            output.accept(value);
            while (!clientCallStream.isReady()) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
            requestStreamRef.get().request(1);
            break;
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.info("Client {} - StreamObserver failed: {}", clientNumber.get(), Throwables.getStackTraceAsString(t));
        doneLatch.countDown();
      }

      @Override
      public void onCompleted() {
        logger.info("Client {} - StreamObserver completed", clientNumber.get());
        doneLatch.countDown();
      }

      @Override
      public void beforeStart(ClientCallStreamObserver<foo.bar.ClientMessage> requestStream) {
        requestStream.disableAutoInboundFlowControl();
        requestStream.setOnReadyHandler(() -> {
          requestStreamRef.set(requestStream);
          if (requestStream.isReady() && connected.compareAndSet(false, true)) {
            doConnect.run();
          }
        });
      }
    });
    connectStreamRef.set(connectStream);
    return connectStream;
  }

  private static void sendWhenReady(ClientCallStreamObserver<foo.bar.ClientMessage> clientCallStream, foo.bar.ClientMessage clientMessage) {
    while (!clientCallStream.isReady()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    clientCallStream.onNext(clientMessage);
    clientCallStream.request(1);
  }

  private static foo.bar.ClientMessage newStreamEndMessage(String sid) {
    return foo.bar.ClientMessage.newBuilder()
        .setStream(foo.bar.ClientStream.newBuilder()
            .setEnd(foo.bar.StreamEnd.newBuilder().setSid(sid)))
        .build();
  }

  private static foo.bar.ClientMessage newStreamItem(String sid) {
    return foo.bar.ClientMessage.newBuilder()
        .setStream(foo.bar.ClientStream.newBuilder()
            .setItem(foo.bar.StreamItem
                .newBuilder().setSid(sid))).build();
  }

  private static foo.bar.ClientMessage newStreamBeginMessage(String sid) {
    return foo.bar.ClientMessage.newBuilder()
        .setStream(foo.bar.ClientStream.newBuilder()
            .setBegin(foo.bar.StreamBegin.newBuilder().setSid(sid)))
        .build();
  }

}
