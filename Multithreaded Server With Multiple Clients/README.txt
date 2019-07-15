Hello

The program is pretty straightforward, it meets all requirements
of the assignment except for logging activity on the server.

I put comments where needed, most the methods describe themselves.

File Structure and Description:
client
    Client: Client node, listens for new messages from server.
    ClientStatistics: Outputs stats for a client every 20 Seconds.
    SenderThread: Started by Client, sends new hash to Server every
        however many seconds.
hash
    Hash: Used by Client and Server to hash bytes.
server
    Server: Sets up TPM and listens for incoming connections/messages.
    ServerStatistics(NOT IMPLEMENTED): Stats about server.
    Task: Task object which all incoming messages/connections become
        to make it easier to transfer them around to different threads.
    ThreadPoolManager: Started by Server, Creates a threadpool and
        assigns tasks in batches to threads.
    WorkerThread: Thread in the threadpool that performs various tasks.


If you have any questions feel free to reach me at ehharris@rams.colostate.edu!