package cs455.scaling.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Server extends Thread {

    private int portNum;
    private ThreadPoolManager tpm;
    private ServerSocketChannel serSocChan;
    private Selector selector;
    private SelectionKey key;
    private ServerStatistics stats;


    public Server(int portNum, int threadPoolSize, int batchSize, double batchTime){
        this.portNum = portNum;
        this.tpm = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);
        this.stats = new ServerStatistics();
        try {
            this.selector =  Selector.open();
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("Error: selector not setup");
        }
    }

    private void setup(){
        this.tpm.start();
        //Stats not implemented
        //this.stats.start();
        try {
            serSocChan = ServerSocketChannel.open();
            serSocChan.socket().bind(new InetSocketAddress(portNum));
            serSocChan.configureBlocking(false);
            int ops = serSocChan.validOps();
            key = serSocChan.register(selector, ops, null);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    //Sends task to tpm
    private void addTask(SelectionKey key, String type){
        tpm.addTask(new Task(key, selector, type));
    }

    //Checks incoming messages
    public void run() {
        while(true){
                try {
                    selector.selectNow();
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        if (key.isAcceptable() && key.isValid()) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_ACCEPT);
                            key.attach(stats);
                            addTask(key, "accept");
                        } else if (key.isReadable() && key.isValid()) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                            key.attach(stats);
                            addTask(key, "read");
                        }
                        iter.remove();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Error, server crashed!");
                }
            }
    }


    public static void main(String[] args) {
        if (args.length != 4){
            System.out.println("Error: INCORRECT USAGE! Usage = java cs455.scaling.server.Server portnum thread-pool-size batch-size batch-time\n");
            System.exit(1);
        } else {
            Server server = new Server(Integer.parseInt(args[0]), Integer.parseInt(args[1]),Integer.parseInt(args[2]),Double.parseDouble(args[3]));
            server.setup();
            server.start();
        }
    }
}
