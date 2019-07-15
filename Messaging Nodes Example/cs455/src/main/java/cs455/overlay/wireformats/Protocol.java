package cs455.overlay.wireformats;

public class Protocol {
    public static final int REGISTER_REQUEST = 1;
    public static final int REGISTER_RESPONSE = 2;
    public static final int DEREGISTER_REQUEST = 3;
    public static final int DEREGISTER_RESPONSE = 4;
    public static final int MESSAGING_NODES_LIST = 5;
    public static final int LINK_WEIGHTS = 6;
    public static final int TASK_INITIATE = 7;
    public static final int RANDOM_PAYLOAD = 8;
    public static final int TASK_COMPLETE = 9;
    public static final int PULL_TRAFFIC_SUMMARY = 10;
    public static final int TRAFFIC_SUMMARY = 11;

}
