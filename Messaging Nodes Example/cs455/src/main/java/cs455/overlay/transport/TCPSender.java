package cs455.overlay.transport;

import cs455.overlay.wireformats.Event;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class TCPSender {

    //Name format = "Hostname:Port"
    private String name;
    private Socket socket;
    private DataOutputStream dout;
    private String connectionName;
    private String localName;

    public TCPSender(Socket socket) throws IOException {
        this.socket = socket;
        this.name = getHostName() + ":" + socket.getLocalPort();
        dout = new DataOutputStream(socket.getOutputStream());
        this.localName = getLocalHostName() + ":" + socket.getLocalPort();
    }

    public void sendMessage(Event event) throws IOException {
        byte[] marshalledBytes = event.getBytes();
        int dataLength = marshalledBytes.length;
        dout.writeInt(dataLength);
        dout.write(marshalledBytes, 0, dataLength);
        dout.flush();

    }
    public String getHostName() {
        String piece = socket.getInetAddress().getHostName();
        String[] pieces = piece.split("\\.");
        return pieces[0];
    }
    public String getName(){
        return name;
    }

    public String getLocalName(){
        return this.localName;
    }
    public int getHostPort(){ return socket.getPort(); }
    public int getLocalPort(){ return socket.getLocalPort(); }
    public String getLocalHostName(){
        try{
            return socket.getInetAddress().getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return "Error No Host Name";
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }
}
