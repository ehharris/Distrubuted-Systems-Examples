package cs455.scaling.server;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public class Task {

    private String type;
    private Selector serSelector;
    private SelectionKey key;

    public Task(String type){
        this.type = type;
    }

    public Task(SelectionKey key, Selector serSelector, String type){
        this.type = type;
        this.serSelector = serSelector;
        this.key = key;
    }

    public String getType() {
        return type;
    }

    public SelectionKey getKey() {
        return key;
    }

    public Selector getSerSelector() {
        return serSelector;
    }
}
