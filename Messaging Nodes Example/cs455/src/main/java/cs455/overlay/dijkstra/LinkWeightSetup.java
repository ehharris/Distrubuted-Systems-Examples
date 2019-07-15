package cs455.overlay.dijkstra;

import cs455.overlay.transport.TCPSender;

import java.util.ArrayList;
import java.util.Random;

public class LinkWeightSetup {

    public static ArrayList<String> setupLinkWeights(ArrayList<TCPSender> registeredNodes, String[] connectionMessages){
        int index = 0;
        Random rand = new Random();
        ArrayList<String> weights = new ArrayList<>();
        //Splits up messages to tell who's connected to who
        for(String message: connectionMessages){
            String[] connections = message.split("\n");
            String link1 = registeredNodes.get(index).getConnectionName();
            for(String link2: connections){
                //Weight limit is 1-10
                int weight = rand.nextInt(11);
                if (weight == 0){
                    weight++;
                }
                String total = link1 + " " + link2 + " " + weight;
                weights.add(total);
            }
            index++;
        }
        return weights;
    }
}
