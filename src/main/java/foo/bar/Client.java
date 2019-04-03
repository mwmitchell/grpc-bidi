package foo.bar;

import com.google.common.base.Throwables;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

public class Client {

  public static void main(String[] args) throws InterruptedException {
    ManagedChannel ch = ManagedChannelBuilder
        .forAddress(Server.host, Server.port)
        .usePlaintext()
        .build();

    foo.bar.FooBarServiceGrpc.FooBarServiceStub stub = foo.bar.FooBarServiceGrpc.newStub(ch);

    int numConnectStreams = 20;

    CountDownLatch doneLatch = new CountDownLatch(numConnectStreams);

    IntStream.range(0, numConnectStreams).forEach(i -> {
      establishConnectCall(i, stub, doneLatch);
    });

    doneLatch.await();
    System.exit(1);
  }

  private static void establishConnectCall(Integer num, foo.bar.FooBarServiceGrpc.FooBarServiceStub stub, CountDownLatch doneLatch) {

    final Logger logger = LoggerFactory.getLogger(Client.class.getName() + "-" + num);

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
            foo.bar.ServerStreamRequest serverStreamRequest = value.getStreamRequest();
            logger.info("Client {} - Stream request for {} items", clientNumber.get(), serverStreamRequest.getNumItems());
            String sid = UUID.randomUUID().toString();
            foo.bar.ClientMessage m = newStreamBeginMessage(sid);
            sendWhenReady(clientCallStream, m);
            clientCallStream.onNext(m);
            IntStream.range(0, serverStreamRequest.getNumItems()).forEach(i -> {
              sendWhenReady(clientCallStream, newStreamItem(sid));
            });
            sendWhenReady(clientCallStream, newStreamEndMessage(sid));
            logger.info("Client {} - Finished sending {} items", clientNumber.get(), serverStreamRequest.getNumItems());
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
