import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.ReferenceCountUtil;

import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;

public class Netty4Benchmark {

  static final ByteBuf PAYLOAD =
      Unpooled.unreleasableBuffer(
          Unpooled.unmodifiableBuffer(
              Unpooled.copiedBuffer(new byte[32])));

  public static final int CPUS = Runtime.getRuntime().availableProcessors();
  public static final PooledByteBufAllocator POOLED_ALLOCATOR = new PooledByteBufAllocator();
  public static final int CONCURRENT_REQUESTS = 1000;

  static class Server {

    public Server(final InetSocketAddress address) throws InterruptedException {
      final EventLoopGroup bossGroup = new NioEventLoopGroup();
      final EventLoopGroup workerGroup = new NioEventLoopGroup();

      final ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(final SocketChannel ch) throws Exception {
              ch.config().setAllocator(POOLED_ALLOCATOR);
              ch.pipeline().addLast(
                  new LengthFieldPrepender(4),
                  new LengthFieldBasedFrameDecoder(128 * 1024 * 1024, 0, 4, 0, 4),
                  new Handler());
            }
          });

      b.bind(address).sync();
    }

    class Handler extends ChannelInboundHandlerAdapter {

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        try {
          ctx.writeAndFlush(PAYLOAD.duplicate());
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }
    }
  }

  static class Client {

    public Client(final InetSocketAddress address)
        throws InterruptedException {

      final EventLoopGroup workerGroup = new NioEventLoopGroup();
      final Bootstrap b = new Bootstrap();
      b.group(workerGroup)
          .channel(NioSocketChannel.class)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(final SocketChannel ch) throws Exception {
              ch.config().setAllocator(POOLED_ALLOCATOR);
              ch.pipeline().addLast(
                  new LengthFieldPrepender(4),
                  new LengthFieldBasedFrameDecoder(128 * 1024 * 1024, 0, 4, 0, 4),
                  new Handler());
            }
          });

      b.connect(address).sync();

    }

    public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
    private long counter;
    public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
    public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

    private class Handler extends ChannelInboundHandlerAdapter {

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
          ctx.write(PAYLOAD.duplicate());
        }
        ctx.flush();
      }

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        try {
          counter++;
          ctx.writeAndFlush(PAYLOAD.duplicate());
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }
    }
  }

  public static void main(final String... args) throws InterruptedException {

    final InetSocketAddress address = new InetSocketAddress(getLoopbackAddress(), 4711);
    final Server server = new Server(address);
    final Client client = new Client(address);

    long prevCounter = 0;
    while (true) {
      final long counter = client.counter;
      final long delta = counter - prevCounter;
      prevCounter = client.counter;
      out.println(delta + " req/s");
      Thread.sleep(1000);
    }
  }
}
