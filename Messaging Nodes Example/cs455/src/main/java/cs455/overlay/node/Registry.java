package cs455.overlay.node;


import cs455.overlay.dijkstra.LinkWeightSetup;
import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.transport.TCPSender;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.OverlayCreator;
import cs455.overlay.wireformats.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class Registry extends Node implements Runnable{

    private TCPServerThread server;
    private ArrayList<TCPSender> registeredNodes;
    private RoutingCache routingCache;
    private String[] connectionMessages;
    private int isOverlaySet;

    public Registry(int port){
        super();
        this.port = port;
        registeredNodes = new ArrayList<>();
        isOverlaySet = 0;
    }

    private void sendMessage(TCPSender toSend, int type, String message){
        Event event = new Event();
        event.setType(type);
        event.setID(toSend.getName());
        event.setMessage(message);
        try {
            toSend.sendMessage(event);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void registerResponse(TCPSender toSend, int success, String info){
        RegisterResponse RR = new RegisterResponse();
        RR.setInfo(info);
        RR.setSuccess(success);
        try {
            toSend.sendMessage(RR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void DeregisterResponse(TCPSender toSend, int success, String info){
        DeregisterResponse DeRR = new DeregisterResponse();
        DeRR.setInfo(info);
        DeRR.setSuccess(success);
        try {
            toSend.sendMessage(DeRR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setupOverlay(int numConnections){
        //Randomizes Connections
        Collections.shuffle(registeredNodes);
        //Get Messages to tell nodes who to connect to
        connectionMessages = OverlayCreator.createOverlay(numConnections, registeredNodes);
        //Sends messages to nodes instructing them
        for (int i = 0; i < connectionMessages.length; i++){
            MessagingNodesList nList = new MessagingNodesList();
            nList.setNumberOfMessingNodes(numConnections);
            nList.setMessagingNodes(connectionMessages[i]);
            nList.setType(Protocol.MESSAGING_NODES_LIST);
            try {
                registeredNodes.get(i).sendMessage(nList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        isOverlaySet = 1;
    }

    private void setupLinkWeights(){
        if (isOverlaySet == 0){
            System.out.println("Overlay is not setup yet! Cannot display or send link weights!");
            return;
        }
        ArrayList<String> linkWeights = LinkWeightSetup.setupLinkWeights(registeredNodes, connectionMessages);
        routingCache = new RoutingCache(linkWeights);
        LinkWeights lWeight = new LinkWeights();
        lWeight.setNumLinks(linkWeights.size());
        lWeight.setLinkWeights(linkWeights);
        lWeight.setType(Protocol.LINK_WEIGHTS);
        for(int i = 0; i < registeredNodes.size(); i++){
            try {
                registeredNodes.get(i).sendMessage(lWeight);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void onEvent(Event event){
        int type = event.getType();
        if (type == Protocol.REGISTER_REQUEST){
            Register reg = (Register) event;
            System.out.println("Request to register from " + reg.getConnectionName());
            TCPSender eventSender = senderList.get(reg.getID());
            eventSender.setConnectionName(reg.getConnectionName());
            if (registeredNodes.contains(eventSender)){
                registerResponse(eventSender, 0, "Register not complete! Already in registry!");
            }else {
                registeredNodes.add(eventSender);
                registerResponse(eventSender, 1, "Register completed!");
            }

        } else if(type == Protocol.DEREGISTER_REQUEST){
            Deregister DReg = (Deregister) event;
            System.out.println("Request to deregister from " + event.getID());
            TCPSender eventSender = senderList.get(event.getID());
            if (registeredNodes.contains(eventSender)){
                registeredNodes.remove(eventSender);
                DeregisterResponse(eventSender, 1, "Deregister complete!");
            }else {
                DeregisterResponse(eventSender, 0, "Deregister not complete! Not in registry!");
            }
        }
    }

    private void commandLine(){
        Scanner keyBoard = new Scanner(System.in);
        System.out.println("Listening for user input");
        while (true){
            String command = keyBoard.nextLine();
            String[] input = command.split(" ", 2);
            switch (input[0]){
                case "list-messaging-nodes":
                    System.out.println("Messaging nodes registered:");
                    for ( TCPSender s : registeredNodes){
                        System.out.println("Host: " + s.getHostName() + " Port: " + s.getHostPort());
                    }
                    break;

                case "list-weights":
                    try {
                        ArrayList<String> linkWeights = routingCache.getLinkWeights();
                        for(String link: linkWeights){
                            System.out.println(link);
                        }
                    } catch (NullPointerException e) {
                        System.out.println("Link weights haven't been setup yet! Run 'setup-overlay-link-weights'");
                    }
                    break;

                case "setup-overlay":
                    int numConnections = 4;
                    if (input.length == 2){
                        numConnections = Integer.parseInt(input[1]);
                    }
                    setupOverlay(numConnections);
                    break;

                case "setup-overlay-link-weights":
                    setupLinkWeights();
                    break;

                    //TODO
                case "start":
                    int numRounds;
                    if (input.length == 2){
                        numRounds = Integer.parseInt(input[1]);
                    }
                    System.out.println("Start hasn't been implemented :(...yet!");
                    break;

                default:
                    System.out.println("Not a valid message!");

            }
        }
    }

    @Override
    public void run() {
        System.out.println("Starting Server");
        server = makeServer();
        Thread t1 = new Thread(server);
        t1.start();
        commandLine();
    }

    public static void main(String args[]){
        //check if correct args
        if (args.length != 1) {
            System.out.println("Error, incorrect num of param. Usage: 'java cs455.overlay.node.Registry portnum'");
            System.exit(-1);
        }
        int port = -1;
        try {
            port = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e){
            System.out.println("Registry takes an int for portnum, this: " + args[0] + "ain't no int!");
            System.exit(-1);
        }
        Registry Registry = new Registry(port);
        Registry.run();

    }
}
