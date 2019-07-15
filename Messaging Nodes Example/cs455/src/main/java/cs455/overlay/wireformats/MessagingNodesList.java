package cs455.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MessagingNodesList extends Event {

    private int numberOfMessingNodes;
    private String messagingNodes;

    public MessagingNodesList(){
        super();
    }

    public void setNumberOfMessingNodes(int numberOfMessingNodes) {
        this.numberOfMessingNodes = numberOfMessingNodes;
    }

    public int getNumberOfMessingNodes() {
        return numberOfMessingNodes;
    }

    public void setMessagingNodes(String messagingNodes) {
        this.messagingNodes = messagingNodes;
    }

    public String getMessagingNodes(){
        return messagingNodes;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        dout.writeInt(type);
        dout.writeInt(numberOfMessingNodes);
        byte[] messageBytes = messagingNodes.getBytes();
        int messageLength = messageBytes.length;
        dout.writeInt(messageLength);
        dout.write(messageBytes);
        dout.flush();
        totalMessage = baOutputStream.toByteArray();
        baOutputStream.close();
        dout.close();
        return totalMessage;
    }
}
