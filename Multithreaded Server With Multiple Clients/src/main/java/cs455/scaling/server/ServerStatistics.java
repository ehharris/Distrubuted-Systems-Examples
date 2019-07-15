package cs455.scaling.server;

import java.util.concurrent.atomic.AtomicInteger;


//NOT IMPLEMENTED, USE AT YOUR OWN RISK

public class ServerStatistics extends Thread {

    private AtomicInteger activeConnections;
    private AtomicInteger messagesProcessed;

    public ServerStatistics(){
        activeConnections = new AtomicInteger(0);
        messagesProcessed = new AtomicInteger(0);
    }

    public void addConnection(){
        activeConnections.getAndIncrement();
    }

    public void messageProcessed(){
        messagesProcessed.getAndIncrement();
    }

    @Override
    public void run() {
        while(true){
            try{
                sleep(20000);
            } catch (InterruptedException i){
                i.printStackTrace();
            }
            int AC = activeConnections.get();
            double MP = (double) messagesProcessed.get();
            double messagesPerSec = MP/20;
            System.out.println("[" + java.time.LocalDateTime.now() + "} Server Throughput: " + messagesPerSec
            + ", Active Client Connections: " + AC);
            activeConnections.set(0);
            messagesProcessed.set(0);

        }

    }
}
