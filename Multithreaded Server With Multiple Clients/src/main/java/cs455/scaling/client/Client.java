package cs455.scaling.client;

import cs455.scaling.hash.Hash;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

public class Client extends Thread {

    private String serverHost;
    private int portNum;
    private final LinkedList<String> hashcodes;
    private SenderThread sender;
    private SocketChannel channel;
    private int messageRate;
    private Selector selector;
    private ClientStatistics stats;

    public Client(String serverHost, int portNum, int messageRate){
        this.serverHost = serverHost;
        this.portNum = portNum;
        this.hashcodes = new LinkedList<>();
        this.messageRate = messageRate;
        this.stats = new ClientStatistics();
    }

    private void register() throws IOException {
        selector = Selector.open();
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(serverHost, portNum));
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_CONNECT);
        while(!channel.finishConnect());
        channel.register(selector, SelectionKey.OP_READ);
        sender = new SenderThread(channel, messageRate, this);
    }

    public void addHash(String hash){
        stats.addSentCount();
        synchronized (hashcodes) {
            hashcodes.add(hash);
        }
    }

    public void checkHash(byte[] recievedHash){
        synchronized (hashcodes){
        String hash = new String(recievedHash);
            if(hashcodes.contains(hash)){
                hashcodes.remove(hash);
            }
        }
        stats.addRecievedCount();
    }


    @Override
    public void run() {
        stats.start();
        sender.start();
        while(true){
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(40);
                    int read = 0;
                    while (buffer.hasRemaining() && read != -1) {
                       // System.out.println(read);
                        read = channel.read(buffer);
                        //System.out.println(buffer);
                    }
                    buffer.rewind();
                    checkHash(buffer.array());

                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static void main(String[] args) {
        if (args.length != 3){
            System.out.println("Error: INCORRECT USAGE! Usage = java cs455.scaling.server.Server portnum thread-pool-size batch-size batch-time\n");
            System.exit(1);
        } else {
            try {
                Client client = new Client(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
                client.register();
                client.run();
            } catch (IOException e){
                e.printStackTrace();
                System.out.println("Probably couldn't connect to server");
            }
        }
        }
}
