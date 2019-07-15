package cs455.scaling.client;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientStatistics extends Thread {

    private AtomicInteger sent;
    private AtomicInteger recieved;

    public ClientStatistics(){
        sent = new AtomicInteger(0);
        recieved = new AtomicInteger(0);
    }

    public void addSentCount(){
        sent.getAndIncrement();
    }

    public void addRecievedCount(){
        recieved.getAndIncrement();
    }

    @Override
    public void run() {
        while(true){
            try {
                sleep(20000);
            } catch (InterruptedException i){
                i.printStackTrace();
            }
            int sentCount = sent.get();
            int recCount = recieved.get();
            System.out.println("[" + java.time.LocalDateTime.now() + "} Total Sent Count: " + sentCount
                    + ", Total Receieved Count: " + recCount);
            sent.set(0);
            recieved.set(0);
        }
    }
}
