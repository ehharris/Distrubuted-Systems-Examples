package cs455.overlay.transport;

import cs455.overlay.node.Node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

public class TCPServerThread implements Runnable{
    private int port;
    private Node OGnode;
    private ServerSocket serSocket;
    private boolean keepRunning;


    public TCPServerThread(int port, Node node){
        this.port = port;
        this.OGnode = node;
        this.keepRunning = true;
        //Create new server socket
        createServer();
    }

    public void createServer(){
        try {
            serSocket = new ServerSocket(this.port);
        } catch (IOException e) {
            //System.out.println("I/O Error creating server with port:" + this.port + " Error: " + e);
            //System.out.println("Trying again with new port num");
            this.port++;
            if(this.port == 65535){
                this.port = 0;
            }
            createServer();
        }
    }

    public void connectTo(String hostName, int port){
        try{
            Socket socket = new Socket(hostName, port);
            OGnode.addSender(new TCPSender(socket));
            TCPRecieverThread recieve = new TCPRecieverThread(socket, OGnode);
            Thread t3 = new Thread(recieve);
            t3.start();
            OGnode.addReciever(recieve);
        } catch (IOException e){
            System.out.println("Connection failed" + e);
        }
    }

    public String getHostName(){
        try {
            return serSocket.getInetAddress().getLocalHost().getHostName();
        }catch (UnknownHostException e){
            System.out.println("HOSTNAMENOTFOUND");
        }
        return "Error";
    }

    public int getPort(){
        return this.port;
    }

    public String getName(){ return this.getHostName() + ":" + this.port; }
    @Override
    public void run() {
        //Runs until shutdown to allow continuous new connections
        while(keepRunning) {
            //Accept new connections
            try {
                System.out.println("Server Socket accepting connections");
                Socket socket = serSocket.accept();
                System.out.println("Server Socket accepted new connection!");
                OGnode.addSender(new TCPSender(socket));
                TCPRecieverThread recieve = new TCPRecieverThread(socket, OGnode);
                Thread t2 = new Thread(recieve);
                t2.start();
                OGnode.addReciever(recieve);
            } catch (IOException e) {
                System.out.println("Error Setting up Socket " + e);
            }
        }
    }

}
