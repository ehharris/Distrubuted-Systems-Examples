package cs455.scaling.server;

import cs455.scaling.hash.Hash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkerThread extends Thread {

    ThreadPoolManager tpm;
    private final ConcurrentLinkedQueue<Task> tasks;
    private Hash hasher;

    public WorkerThread(ThreadPoolManager tpm){
        this.tpm = tpm;
        hasher = Hash.getInstance();
        this.tasks = new ConcurrentLinkedQueue<>();
        tasks.add(new Task("waiting"));
    }

    //Gives tasks to thread from tpm
    public void giveTasks(ConcurrentLinkedQueue<Task> tasks){
       synchronized (this.tasks) {
           this.tasks.clear();
           for (Task t: tasks) {
               this.tasks.add(t);
           }
           this.tasks.notify();
       }
    }

    //After no more tasks we go back to waiting
    private void done() {
        synchronized (tasks) {
            tasks.clear();
            tasks.add(new Task("waiting"));
            tpm.continueSwimming(this);
        }
    }

    //Method for accepting connections
    private void accept(Task task){
        try {
            SelectionKey key = task.getKey();
            Selector serSelector = key.selector();
            ServerSocketChannel serChannel = (ServerSocketChannel) key.channel();
            key.interestOps(key.interestOps() | SelectionKey.OP_ACCEPT);
            ServerStatistics stats = (ServerStatistics) task.getKey().attachment();
            if (key.isAcceptable()) {
                    SocketChannel channel = serChannel.accept();
                    if(channel != null) {
                        channel.configureBlocking(false);
                        channel.register(serSelector, SelectionKey.OP_READ);
                    }
                }
        } catch (ClosedChannelException c){
            c.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    //Method for reading messages
    private void read(Task task){
        SelectionKey key = task.getKey();
        SocketChannel channel = (SocketChannel) key.channel();
        int read = 0;
        ByteBuffer buffer = ByteBuffer.allocate(8000);
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        try {
                while (buffer.hasRemaining() && read != -1) {
                    read = channel.read(buffer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        if (read == -1){
            System.out.println("Error reading from Socket!");
            return;
        }
        buffer.rewind();
        byte[] byteToHash = buffer.array();
        String response = hasher.toHash(byteToHash);
        write(channel, response, task);
    }

    //Method for writing tasks
    //Currently only called by read() after a read is performed
    private void write(SocketChannel channel, String response, Task task){
        try {
            ByteBuffer hash = ByteBuffer.wrap(response.getBytes());
                channel.register(task.getSerSelector(), SelectionKey.OP_WRITE);
                while (hash.hasRemaining()) {
                    channel.write(hash);
                }
                //Re-register channel to read so it can keep recieving messages
                channel.register(task.getSerSelector(), SelectionKey.OP_READ);
                channel.register(task.getSerSelector(), task.getKey().interestOps() & SelectionKey.OP_READ);
        } catch (ClosedChannelException c){
            c.printStackTrace();
            System.out.println("Error, couldn't register to write/read!");
            return;
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("Error, couldn't write to client!");
            return;
        }
    }


    //Finds Tasks to be processed
    //"waiting" task is to have the thread wait for more tasks
    @Override
    public void run() {
        while (true) {
            synchronized (tasks) {
                while (tasks.peek() != null) {
                    Task newTask = tasks.poll();
                    switch (newTask.getType()) {
                        case "waiting":
                            try {
                                tasks.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            break;

                        case "accept":
                            accept(newTask);
                            break;

                        case "read":
                             read(newTask);
                            break;

                        default:
                            break;
                    }
                }
                done();
            }
        }
    }
}
