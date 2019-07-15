package cs455.overlay.wireformats;

public class Message {
    private String message;

    public Message(String message){
        this.message = message;
    }

    public void setMessage(String message){
        this.message = message;
    }

    public byte[] getDataInBytes(){
        return this.message.getBytes();
    }

    public String getData(){
        return this.message;
    }
}
