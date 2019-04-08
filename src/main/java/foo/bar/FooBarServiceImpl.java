package foo.bar;

import com.google.common.base.Throwables;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FooBarServiceImpl extends foo.bar.FooBarServiceGrpc.FooBarServiceImplBase {

  ConcurrentMap<String, ClientCallHandler> clients = new ConcurrentHashMap<>();

  static long startTime = System.nanoTime();
  static AtomicLong clientMessageCount = new AtomicLong();

  @Override
  public StreamObserver<foo.bar.ClientMessage> connect(StreamObserver<foo.bar.ServerMessage> responseObserver) {
    ServerCallStreamObserver<foo.bar.ServerMessage> serverCallStreamObserver = (ServerCallStreamObserver<foo.bar.ServerMessage>) responseObserver;
    return new ClientCallHandler(clients, serverCallStreamObserver);
  }

  /**
   * Instances of this class are responsible for dealing with a single message from a serverCallStreamObserver.
   */
  public static class ClientCallHandler implements StreamObserver<foo.bar.ClientMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ClientCallHandler.class);

    private final ServerCallStreamObserver<foo.bar.ServerMessage> serverCallStreamObserver;
    private final ConcurrentMap<String, ClientCallHandler> clientRegistry;
    private String clientId;
    private final AtomicBoolean wasReady = new AtomicBoolean();

    public ClientCallHandler(
        ConcurrentMap<String, ClientCallHandler> clientRegistry,
        ServerCallStreamObserver<foo.bar.ServerMessage> serverCallStreamObserver
    ) {
      this.clientRegistry = clientRegistry;
      this.serverCallStreamObserver = serverCallStreamObserver;
      serverCallStreamObserver.disableAutoInboundFlowControl();
      serverCallStreamObserver.setOnReadyHandler(() -> {
        if (serverCallStreamObserver.isReady() && wasReady.compareAndSet(false, true)) {
          serverCallStreamObserver.request(1);
        }
      });
    }

    private void sendStreamRequest() {
      serverCallStreamObserver.onNext(foo.bar.ServerMessage.newBuilder()
          .setStreamRequest(foo.bar.ServerStreamRequest.newBuilder()
              .setNumItems(ThreadLocalRandom.current().nextInt(0, 10000)).build())
          .build());
    }

    @Override
    public void onNext(foo.bar.ClientMessage value) {
      long current = clientMessageCount.incrementAndGet();
      long diff = System.nanoTime() - startTime;
      long diffSeconds = TimeUnit.SECONDS.convert(diff, TimeUnit.NANOSECONDS);
      if(current >= 10_000_000){
        logger.info("Received call-number={}, running-time={} avgPerSec={}", current, diffSeconds, (current/diffSeconds));
        throw new RuntimeException("done!");
      }
      switch (value.getMsgCase()) {
        case CONNECT:
          handleConnect(value);
          sendStreamRequest();
          break;
        case STREAM:
          handleClientMessage(value);
          if (value.getStream().getStreamCase().equals(foo.bar.ClientStream.StreamCase.END)) {
            sendStreamRequest();
          }
          break;
        case MSG_NOT_SET:
          handleMsgNotSet();
      }
      if (serverCallStreamObserver.isReady()) {
        serverCallStreamObserver.request(1);
      } else {
        wasReady.set(false);
      }
    }

    private void handleConnect(foo.bar.ClientMessage value) {
      this.clientId = value.getConnect().getCid();
      // Register the "serverCallStreamObserver", which is this class (ClientCallHandler),
      // using the provided "cid" (clientId).
      if (clientRegistry.putIfAbsent(clientId, this) != null) {
        serverCallStreamObserver.onError(Status.ALREADY_EXISTS.asException());
      } else {
        // Send and ACK to the serverCallStreamObserver:
        serverCallStreamObserver.onNext(foo.bar.ServerMessage.newBuilder()
            .setConnectReply(foo.bar.ServerConnectReply.newBuilder()).build());
      }
    }

    private void handleClientMessage(foo.bar.ClientMessage value) {
      // Do nothing...
    }

    private void handleMsgNotSet() {
      // Do nothing...
    }


    @Override
    public void onError(Throwable t) {
      logger.error("Client Error: {} / {}", clientId, Throwables.getStackTraceAsString(t));
      clientRegistry.remove(clientId);
    }

    @Override
    public void onCompleted() {
      logger.info("Client Completed: {}", clientId);
      clientRegistry.remove(clientId);
    }
  }

}
