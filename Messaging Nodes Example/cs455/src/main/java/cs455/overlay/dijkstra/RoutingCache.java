package cs455.overlay.dijkstra;

import java.util.ArrayList;

public class RoutingCache {

    private ArrayList<String> linkWeights;

    public RoutingCache(ArrayList<String> linkWeights){
        this.linkWeights = linkWeights;
    }

    public ArrayList<String> getLinkWeights(){
        return linkWeights;
    }

}
