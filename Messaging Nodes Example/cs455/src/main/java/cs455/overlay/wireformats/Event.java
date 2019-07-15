package cs455.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Event {
    public byte[] totalMessage;
    public Message actualMessage;
    public int type;
    public String ID;

    //TODO: add type and message functionality
    public Event(){
        totalMessage = null;
    }

    public Event(byte[] event){
        totalMessage = event;
    }

    //TODO: marshall the bytes
    public byte[] getBytes() throws IOException {
        return new byte[8];
    }

    public int getType(){
        return type;
    }

    public void setType(int type){
        this.type = type;
    }

    public void setID(String identifier){
        this.ID = identifier;
    }

    public String getID(){
        return this.ID;
    }
    //creates message
    public void setMessage(String data){
        actualMessage = new Message(data);
    }

    public Message getMessage(){
        return actualMessage;
    }
}
