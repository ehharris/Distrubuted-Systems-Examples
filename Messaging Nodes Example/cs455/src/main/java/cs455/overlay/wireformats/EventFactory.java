package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class EventFactory {

    private static final EventFactory instance = new EventFactory();

    private EventFactory(){}

    public Event decodeEvent(byte[] marshalledRecieved)throws IOException{
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledRecieved);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
        int type = din.readInt();
        switch (type){
            case Protocol.REGISTER_REQUEST:
                Register reg = new Register();
                reg.setType(type);
                int identifierLength = din.readInt();
                byte[] identifierBytes = new byte[identifierLength];
                din.readFully(identifierBytes);
                reg.setID(new String(identifierBytes));
                int connectionLength = din.readInt();
                byte[] connectionBytes = new byte[connectionLength];
                din.readFully(connectionBytes);
                reg.setConnectionName(new String(connectionBytes));
                return reg;
            case Protocol.REGISTER_RESPONSE:
                RegisterResponse regR = new RegisterResponse();
                regR.setType(type);
                regR.setSuccess(din.readInt());
                int infoLength = din.readInt();
                byte[] infoBytes = new byte[infoLength];
                din.readFully(infoBytes);
                regR.setInfo(new String(infoBytes));
                return regR;
            case Protocol.DEREGISTER_REQUEST:
                Deregister DReg = new Deregister();
                DReg.setType(type);
                int idLength = din.readInt();
                byte[] idBytes = new byte[idLength];
                din.readFully(idBytes);
                DReg.setID(new String(idBytes));
                return DReg;
            case Protocol.MESSAGING_NODES_LIST:
                MessagingNodesList mList = new MessagingNodesList();
                mList.setType(type);
                mList.setNumberOfMessingNodes(din.readInt());
                int messLength = din.readInt();
                byte[] messBytes = new byte[messLength];
                din.readFully(messBytes);
                mList.setMessagingNodes(new String (messBytes));
                return mList;
            case Protocol.LINK_WEIGHTS:
                LinkWeights lWeight = new LinkWeights();
                lWeight.setType(type);
                lWeight.setNumLinks(din.readInt());
                ArrayList<String> linkWeights = new ArrayList<>();
                while (din.available() > 0){
                    int Length = din.readInt();
                    byte[] connection = new byte[Length];
                    din.readFully(connection);
                    linkWeights.add(new String(connection));
                }
                lWeight.setLinkWeights(linkWeights);
                return lWeight;

        }
        //Shouldn't get to here unless an error
        return new Event();
    }



    public static EventFactory getInstance(){
        return instance;
    }



}
