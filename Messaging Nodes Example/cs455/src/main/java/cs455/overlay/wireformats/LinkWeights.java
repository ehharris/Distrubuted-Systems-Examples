package cs455.overlay.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class LinkWeights extends Event{

    private int numLinks;
    private ArrayList<String> linkWeights;

    public int getNumLinks() {
        return numLinks;
    }

    public void setNumLinks(int numLinks) {
        this.numLinks = numLinks;
    }

    public ArrayList<String> getLinkWeights() {
        return linkWeights;
    }

    public void setLinkWeights(ArrayList<String> linkWeights) {
        this.linkWeights = linkWeights;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        dout.writeInt(type);
        dout.writeInt(numLinks);
        for(String link: linkWeights){
            byte[] linkBytes = link.getBytes();
            int linkLength = linkBytes.length;
            dout.writeInt(linkLength);
            dout.write(linkBytes);
        }
        dout.flush();
        totalMessage = baOutputStream.toByteArray();
        baOutputStream.close();
        dout.close();
        return totalMessage;

    }
}
