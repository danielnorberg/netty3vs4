import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static java.lang.System.out;
import static java.net.InetAddress.getLoopbackAddress;
import static org.jboss.netty.buffer.ChannelBuffers.unmodifiableBuffer;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

public class Netty3Benchmark {

  static final ChannelBuffer PAYLOAD = unmodifiableBuffer(wrappedBuffer(new byte[32]));
  public static final int CPUS = Runtime.getRuntime().availableProcessors();
  public static final int CONCURRENCT_REQUESTS = 1000;

  static class Server {

    public Server(final InetSocketAddress address) {
      final NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), CPUS);

      final ServerBootstrap b = new ServerBootstrap(channelFactory);
      b.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline(new LengthFieldPrepender(4),
                                   new LengthFieldBasedFrameDecoder(128 * 1024 * 1024, 0, 4, 0, 4),
                                   new Handler());
        }
      });

      b.bind(address);
    }

    class Handler extends SimpleChannelUpstreamHandler {

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
          throws Exception {
        e.getChannel().write(PAYLOAD.duplicate());
      }
    }
  }

  static class Client {

    public Client(final InetSocketAddress address)
        throws InterruptedException {
      final ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), CPUS);

      final ClientBootstrap b = new ClientBootstrap(channelFactory);
      b.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline(
              new LengthFieldPrepender(4),
              new LengthFieldBasedFrameDecoder(128 * 1024 * 1024, 0, 4, 0, 4),
              new Handler()
          );
        }
      });
      b.connect(address);
    }

    public volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    public volatile long q0, q1, q2, q3, q4, q5, q6, q7;
    private long counter;
    public volatile long r0, r1, r2, r3, r4, r5, r6, r7;
    public volatile long s0, s1, s2, s3, s4, s5, s6, s7;

    private class Handler extends SimpleChannelUpstreamHandler {

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
          throws Exception {
        final Channel channel = e.getChannel();
        for (int i = 0; i < CONCURRENCT_REQUESTS; i++) {
          channel.write(PAYLOAD.duplicate());
        }
      }

      @Override
      public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
          throws Exception {
        counter++;
        e.getChannel().write(PAYLOAD.duplicate());
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
