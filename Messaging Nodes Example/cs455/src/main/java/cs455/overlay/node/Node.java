package cs455.overlay.node;

import cs455.overlay.transport.TCPRecieverThread;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.wireformats.Event;

import java.net.Socket;
import java.util.Hashtable;


public abstract class Node {
    public int port;
    public Hashtable<String, TCPSender> senderList;
    public Hashtable<String, TCPRecieverThread> recieverList;

    public Node(){
        senderList = new Hashtable<String, TCPSender>();
        recieverList = new Hashtable<String, TCPRecieverThread>();
        port = 0;
    }

    public Node(int port) {
        senderList = new Hashtable<String, TCPSender>();
        recieverList = new Hashtable<String, TCPRecieverThread>();
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void onEvent(Event event){
    }

    public void addSender(TCPSender newSender){
        senderList.put(newSender.getLocalHostName() + ":" + newSender.getHostPort(), newSender);
    }
    public void addReciever(TCPRecieverThread newReciever){
        recieverList.put(newReciever.getName(), newReciever);
    }


    public void sendMessage(){}

    public String listSenders(){
        return senderList.toString();
    }

    public String listRecievers(){
        return recieverList.toString();
    }
    //Creates/Starts TCPServerThread
    public TCPServerThread makeServer(){
        TCPServerThread server = new TCPServerThread(this.port, this);
        //incase port given was in use
        this.port = server.getPort();
        return server;
    }
    public TCPServerThread makeServer(int port){
        TCPServerThread server = new TCPServerThread(port, this);
        //incase port given was in use
        this.port = server.getPort();
        return server;
    }
}
