package cs455.scaling.client;

import cs455.scaling.hash.Hash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;

public class SenderThread extends Thread {

    private final SocketChannel channel;
    private int messageRate;
    private Client client;
    private Random rand;
    private Hash hasher;

    public SenderThread(SocketChannel channel, int messageRate, Client client){
        this.messageRate = messageRate;
        this.client = client;
        rand = new Random();
        hasher = Hash.getInstance();
        this.channel = channel;
    }

    private void sendMessage(byte[] hashNum) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(hashNum);
           // System.out.println(buffer);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
    }

    private byte[] generateHashCode(){
        byte[] buffer = new byte[8000];
        rand.nextBytes(buffer);
        client.addHash(hasher.toHash(buffer));
        //System.out.println(buffer[7999]);
        return buffer;
    }

    @Override
    public void run() {
        while(true){
            try {
                sleep(1000 / messageRate);
            } catch (InterruptedException i){
                i.printStackTrace();
            }
            byte[] hashNum = generateHashCode();
            try {
                sendMessage(hashNum);
            } catch (IOException e){
                e.printStackTrace();
                System.out.println("write failed somehow");
            }
        }

    }
}
