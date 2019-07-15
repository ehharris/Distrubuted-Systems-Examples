package cs455.overlay.util;

import cs455.overlay.transport.TCPSender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class OverlayCreator {

    public static String[] createOverlay(int numConnections, ArrayList<TCPSender> registeredNodes){
        /**registeredNodes gets randomized before it gets here
         * so if called multiple times the nodes connecting to each other
         * be different
         **/

        String[] messages = new String[registeredNodes.size()];
        Arrays.fill(messages, "");
        //Makes sure all nodes can reach each other/no islands of nodes
        //All nodes have 2 connections after this, would be impossible with just one so not planning for it
        for(int i = 0; i < numConnections; i++){
            for(int currNode = 0; currNode < registeredNodes.size(); currNode++){
                int newNode = (currNode + i + 1) % (registeredNodes.size());
                messages[currNode] += registeredNodes.get(newNode).getConnectionName() + "\n";
            }
        }
//
//
//        for (int i = 0; i < registeredNodes.size() - 1; i++){
//            if(i ==  0){
//                connectionCounts[i]++;
//                connectionCounts[registeredNodes.size()-1]++;
//                messages[i] += registeredNodes.get(registeredNodes.size() - 1).getHostName() + ":"
//                        + registeredNodes.get(registeredNodes.size() - 1).getHostPort() + "\n";
//                messages[registeredNodes.size() - 1] += registeredNodes.get(i).getHostName() + ":"
//                        + registeredNodes.get(i).getHostPort() + "\n";
//            }
//            connectionCounts[i]++;
//            connectionCounts[i+1]++;
//            messages[i] += registeredNodes.get(i+1).getHostName() + ":"
//                    + registeredNodes.get(i+1).getHostPort() + "\n";
//            messages[i+1] += registeredNodes.get(i).getHostName() + ":"
//                    + registeredNodes.get(i).getHostPort() + "\n";
//        }
//
//        //Fill rest of connections "randomly" if more than 2 connections is allowed
//        for (int i = 0; i < numConnections - 2; i++){
//            for (int count = 0; count < registeredNodes.size()/2; count++){
//                if (connectionCounts[count] == i+1){
//                    continue;
//                }
//                int newNode = count + i;
//                if (newNode > )
//                connectionCounts[count]++;
//                connectionCounts[newNode]++;
//                messages[count] += registeredNodes.get(newNode).getHostName() + ":"
//                        + registeredNodes.get(newNode).getHostPort() + "\n";
//                messages[newNode] += registeredNodes.get(count).getHostName() + ":"
//                        + registeredNodes.get(count).getHostPort() + "\n";
//            }
//
//        }


//        if(numConnections > 2){
//            for (int i = 0; i < registeredNodes.size() - 1; i++){
//                if (connectionCounts[i] == numConnections){
//                    continue;
//                }
//                //This ensures it doesn't connect to a node it's already connected to or itself
//                int connected[] = new int[numConnections + 1];
//
//                connected[0] = i;
//                connected[1] = i+1;
//                connected[2] = i-1;
//                int connectedCount = 3;
//                if(i == 0){
//                    connected[2] = registeredNodes.size() - 1;
//                }
//                if(i == registeredNodes.size() - 1){
//                    connected[1] = 0;
//                }
//                Random random = new Random();
//                while (connectionCounts[i] != numConnections){
//                    int rand = random.nextInt(registeredNodes.size());
//                    //System.out.println(connectionCounts[rand]);
//
//                    if(check(connected, rand)){
//                        continue;
//                    }
//                    if(connectionCounts[rand] == numConnections){
//                        continue;
//                    }
//                    connected[connectedCount] = rand;
//                    connectedCount++;
//                    connectionCounts[i]++;
//                    connectionCounts[rand]++;
//                    messages[i] += registeredNodes.get(rand).getHostName() + ":"
//                            + registeredNodes.get(rand).getHostPort() + "\n";
//                    messages[rand] += registeredNodes.get(i).getHostName() + ":"
//                            + registeredNodes.get(i).getHostPort() + "\n";
//                }
//            }
//        }
        return messages;
    }

    private static boolean check(int[] connected, int rand){
        boolean check = false;
        for (int i : connected){
            if (i == rand){
                check = true;
            }
        }
        return check;
    }
}
