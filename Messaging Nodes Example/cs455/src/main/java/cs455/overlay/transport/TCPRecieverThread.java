package cs455.overlay.transport;

import cs455.overlay.node.Node;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

public class TCPRecieverThread implements Runnable {
    private static Socket socket;
    private Node node;
    private DataInputStream inputStream;
    private String senderName;
    private EventFactory eventFactory;

    public TCPRecieverThread(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        this.senderName = getHostName();
        this.inputStream = new DataInputStream(socket.getInputStream());
        this.eventFactory = EventFactory.getInstance();
    }

    private static String getHostName(){
        String name = "";
        InetAddress addy = socket.getInetAddress();
        try{
            name = addy.getLocalHost().getHostName();
        } catch (IOException e){
            e.printStackTrace();
        }
        return name;
    }
    private void dealWithEvent(byte[] marshalledRecieved){
        try {
            Event event = eventFactory.decodeEvent(marshalledRecieved);
            node.onEvent(event);
        }catch (EOFException e){
            System.out.println("Event not handled properly ");
            e.printStackTrace();
        }catch (IOException f){
            f.printStackTrace();
        }
    }

    public String getName(){
        return senderName + ":" + socket.getPort();
    }

    @Override
    public void run() {
        System.out.println("Accepting messages");
        //Connection stays active
        while(socket.isConnected()){
            try{
                int size = inputStream.readInt();
                byte[] marshalledRecieved = new byte[size];
                inputStream.readFully(marshalledRecieved, 0, size);
                dealWithEvent(marshalledRecieved);
                System.out.println("Message successfully recieved!");
            } catch (IOException e){
                //Would print this error out but it clogs up the commandline when a node disconnects
                //System.out.println("Message not successfully recieved! " + e);
            }
        }
    }
}
