package cs455.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RegisterResponse extends Event {

    private int type;
    private int success;
    private String info;
    public RegisterResponse(){
        super();
        type = 2;
    }

    public void setSuccess(int isSuccessful){
        success = isSuccessful;
    }

    public int getSuccess() {
        return success;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getInfo(){
        return info;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        dout.writeInt(type);
        dout.writeInt(success);
        byte[] infoBytes = info.getBytes();
        int elementLength = infoBytes.length;
        dout.writeInt(elementLength);
        dout.write(infoBytes);
        dout.flush();
        totalMessage = baOutputStream.toByteArray();
        baOutputStream.close();
        dout.close();
        return totalMessage;
    }
}
