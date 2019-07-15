How to run:

1. Make sure you have gradle installed
2. Navigate to ../cs455
3. Run "gradle build" to build the program
4. Navigate to ../cs455/build/classes/java/main
5. Run "java cs455.overlay.node.Registry portnum" to start the Registry. Registry must be started before any messaging nodes
6. Run "java cs455.overlay.node.MessagingNode registryhost registryport" to start a messaging node. Program should be able to handle as many nodes as you wish

Classes Description (src/main/java/cs455/overlay):
Dijkstra:
    LinkWeights: Assigns link weights to registered nodes when registry command "setup-overlay-link-weights" is run.
    RoutingCache: Used by Registry and MessagingNodes to store link weights when given
    ShortestPath(not implemented): Used to store shortest paths to each node relative to the current nodes and link weights.
Node:
    MessagingNode: Node that connects to registry and other MessagingNodes to send messages between each other.
    Node: Base Node object that Registry and MessagingNode take from
    Registry: Registry for all connected Nodes, instructs connected nodes what to do and keeps track of data
        Commands:
        "list-messaging-nodes": Lists nodes currently in the registry
        "list-weights": Lists link weights for connections between nodes if link weights have been assgined.
        "setup-overlay": Determines which nodes should connect to each other and directs nodes to connect
        "setup-overlay-link-weights": Assgins link weights to MessagingNode connections if setup-overlay has been run.
        "start numofrounds"(not implemented): Tells nodes to start sending "numofrounds" messages to each other
transport:
    TCPRecieverThread: Creates a thread with each new connection to listen for incoming messages.
    TCPSender: Stores a socket to a node to send messages.
    TCPServerThread: Listens for new connections from other nodes.
util:
    OverlayCreator: Creates overlay when command "setup-overlay" is ran in Registry
    StatisticsColletorAndDisplay(not implemented): Stores and displays message stats when nodes are done sending messages.
wireformats:
    Deregister: Holds format for deregister message.
    DeregisterResponse: Holds format for response to deregister message.
    Event: Object that all messages take from
    EventFactory: Decodes and assigns messages that are recieved.
    LinkWeights: Holds format for linkweights message
    Message: Holds format for message
    MessagingNodeList: Holds format for message sent to instruct MessagingNodes to connect to one another
    Protocol: Defines int's for message types
    Register: : Holds format for Register Request.
    RegisterResponse: Holds format for Register Request Response.
    TaskComplete(not implemented):Holds format for Task Complete message.
    TaskInitiate(not implemented): Holds format for Task Initiate message.
    TaskSummaryRequest(not implemented): Holds Format for summary info request
    TaskSummaryRequestResponse(not implemented): Holds Format for summary info request response