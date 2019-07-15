package cs455.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Register extends Event {

    private String connectionName;

    public Register(){
        super();
    }

    public void setConnectionName(String connectionName){
        this.connectionName = connectionName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        dout.writeInt(type);
        byte[] identifierBytes = ID.getBytes();
        int elementLength = identifierBytes.length;
        dout.writeInt(elementLength);
        dout.write(identifierBytes);
        byte[] connectionBytes = connectionName.getBytes();
        int cLength = connectionBytes.length;
        dout.writeInt(cLength);
        dout.write(connectionBytes);
        dout.flush();
        totalMessage = baOutputStream.toByteArray();
        baOutputStream.close();
        dout.close();
        return totalMessage;
    }
}
