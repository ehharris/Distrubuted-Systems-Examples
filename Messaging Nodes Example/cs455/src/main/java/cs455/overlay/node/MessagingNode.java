package cs455.overlay.node;

import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.transport.TCPRecieverThread;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.wireformats.*;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class MessagingNode extends Node implements Runnable {
    private String regHost;
    private int regPort;
    private Socket registry;
    private TCPServerThread server;
    private TCPSender regSender;
    private TCPRecieverThread regReciever;
    private RoutingCache routingCache;

    public MessagingNode(String regHost, int regPort){
        super();
        this.regHost = regHost;
        this.regPort = regPort;
    }

    private synchronized void registerRequest(){
        Register register = new Register();
        register.setType(Protocol.REGISTER_REQUEST);
        register.setID(regSender.getName());
        register.setConnectionName(server.getName());
        try {
            regSender.sendMessage(register);
            System.out.println("Sent Request to Register!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deregisterRequest(){
        Deregister deRegister = new Deregister();
        deRegister.setType(Protocol.DEREGISTER_REQUEST);
        deRegister.setID(regSender.getName());
        try {
            regSender.sendMessage(deRegister);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connectToNodes(MessagingNodesList mList){
        String nodes[] = mList.getMessagingNodes().split("\n");
        for(String node: nodes){
            String[] fullName = node.split(":");
            System.out.println(node);
            server.connectTo(fullName[0], Integer.parseInt(fullName[1]));
        }
    }

    private void storeLinkWeights(LinkWeights lWeight){
        routingCache = new RoutingCache(lWeight.getLinkWeights());

    }

    public void onEvent(Event event){
        switch (event.getType()){
            case Protocol.REGISTER_RESPONSE:
                RegisterResponse RR = (RegisterResponse) event;
                System.out.println(RR.getInfo());
                break;
            case Protocol.DEREGISTER_RESPONSE:
                DeregisterResponse DeRR = (DeregisterResponse) event;
                System.out.println(DeRR.getInfo());
                break;
            case Protocol.MESSAGING_NODES_LIST:
                MessagingNodesList mList = (MessagingNodesList) event;
                connectToNodes(mList);
                break;
            case Protocol.LINK_WEIGHTS:
                LinkWeights lWeight = (LinkWeights) event;
                storeLinkWeights(lWeight);
                break;
        }
    }

    public static void main(String args[]){
        //Check if proper host/port
        if (args.length != 2) {
            System.out.println("Error, incorrect num of param. Usage: 'java cs455.overlay.node.MessagingNode registryHost registryPort'");
            System.exit(-1);
        }
        String regHost = args[0];
        int regPort = 0;
        try {
             regPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("MessagingNode takes an int for registryPort, this:  " + args[1] + " ain't no int!");
            System.exit(-1);
        }
        MessagingNode MN = new MessagingNode(regHost, regPort);
        MN.run();
    }

    private void commandLine(){
        Scanner keyBoard = new Scanner(System.in);
        System.out.println("Listening for user input");
        while (true){
            String command = keyBoard.nextLine();
            switch (command){
                case "print-shortest-path":
                    System.out.println("NOPE");
                    break;
                case "exit-overlay":
                    System.out.println("Maybe someday");
                    break;
                default:
                    System.out.println("Not a valid command!");

            }
        }
    }

    @Override
    public void run() {

        try {
            registry = new Socket(regHost, regPort);
            regSender = new TCPSender(registry);
            regReciever = new TCPRecieverThread(registry, this);
            Thread t1 = new Thread(regReciever);
            t1.start();
        }
        catch (IOException e){
            System.out.println("Couldn't connect to registry");
        }
        server = makeServer(55555);
        Thread t2 = new Thread(server);
        t2.start();
        registerRequest();
        commandLine();

    }
}
