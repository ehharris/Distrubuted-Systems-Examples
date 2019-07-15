package cs455.scaling.server;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;


public class ThreadPoolManager extends Thread {

    private final int poolSize;
    private final int batchSize;
    private final double batchTime;
    private final ConcurrentLinkedQueue<Task> taskQueue;
    private final LinkedList<WorkerThread> threadPool;

    public ThreadPoolManager(int poolSize, int batchSize, double batchTime){
        this.poolSize = poolSize;
        this.batchSize = batchSize;
        this.batchTime = batchTime;
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.threadPool = new LinkedList<>();
        for (int i=0; i < poolSize; i++) {
            threadPool.add(new WorkerThread(this));
        }
        startThreads();
    }

    private void startThreads(){
        for (WorkerThread i : threadPool){
            i.start();
        }
    }

    //Method for server to add to taskQueue
    public void addTask(Task task){
        taskQueue.add(task);
    }

    //Method to tell tpm a thread is ready again
    public void continueSwimming(WorkerThread worker){
        synchronized (threadPool){
            threadPool.add(worker);
            threadPool.notify();
        }
    }

    //Polls and sees if there's any threads, if not it waits
    private WorkerThread getWorker(){
        WorkerThread nextThread;
        synchronized (threadPool){
            nextThread = threadPool.poll();
            if (nextThread == null){
                try{
                    threadPool.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                nextThread = threadPool.poll();
            }
        }
        return nextThread;
    }

    //Sends a batch to a thread
    private void sendBatch(ConcurrentLinkedQueue<Task> batchQueue){
        WorkerThread nextThread = getWorker();
        nextThread.giveTasks(batchQueue);

    }

    //Processes messages
    //Keeps track of batch-size/batch-time
    @Override
    public void run() {
        while(true) {
                ConcurrentLinkedQueue<Task> batchQueue = new ConcurrentLinkedQueue<>();
                int batchQueueSize = 0;
                long batchStartTime = (System.currentTimeMillis() / 1000);

                    //While loop is for batch-time/batch-size
                    while ((batchQueueSize < batchSize) && (((System.currentTimeMillis() / 1000) - batchStartTime) < batchTime)) {
                        Task nextTask = taskQueue.poll();
                        if (nextTask != null) {
                            batchQueue.add(nextTask);

                            //Added this so an when accepting the first clients it doesn't have to wait for (batchSize) clients
                            //to connect to connect them
                            if (nextTask.getType().equals("accept")) {
                                break;
                            } else {
                                batchQueueSize++;
                            }
                        }
                    }
                    //Doesn't send empty batches
                    if (batchQueue.size() != 0) {
                        sendBatch(batchQueue);
                    }
        }
    }

    //DEPRECIATED
    //This is just for testing purposes
    //Starts a thread pool of 4 threads and sends 10 meaningless tasks
    public static void main(String[] args) {
        //ThreadPoolManager tpm = new ThreadPoolManager(4, 0, 0);
        //tpm.start();
    try {
        sleep(1000);
    }catch (InterruptedException e){
        e.printStackTrace();
    }
    for (int i = 0; i < 10; i++){
           // tpm.addTask(new Task());
        }


    }
}
