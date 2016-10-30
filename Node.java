import dcs.os.Server;
import dcs.os.StockList;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.*;

public class Node {

  public static void usage() {
    System.out.println("Usage:");
    System.out.println("\tjava Node server <server port> <client port> <coordinator server> <database path> <cohort size>");
    System.out.println("\t\t<server port>: The port that the server listens on for other servers");
    System.out.println("\t\t<client port>: The port that the server listens on for clients");
    System.out.println("\t\t<other servers>: A comma separated list of the other server's addresses and ports");
    System.out.println("\t\t\tExamples:");
    System.out.println("\t\t\t - 'localhost:9001'");
    System.out.println("\t\t\t - 'localhost:9001,127.0.0.1:9002'");
    System.out.println("\t\t<database path>: The path to the database file, this needs to be unique per server");
    System.out.println("\t\t\tExamples:");
    System.out.println("\t\t\t - 'db1.txt'");
    System.out.println("\t\t<cohort size>: The number of slve servers to connect with");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    //There are two allowed inputs for server type: coordinator or slave.
    //If coordinator, stup a coordinator, else set up a slave
    String role = args[0];
    if (role.equals("coordinator")) {
      setUpCoordinator(args);
    } else if (role.equals("slave")) {
      setUpSlave(args);
    } else {
      usage();
    }
  }



  //Setup coordinator function will create a coordinator server which will control the other servers.

  private static void setUpCoordinator(String[] args) throws InterruptedException {

    // The port this server should listen on to accetp connections from the client
    int clientListenPort = Integer.parseInt(args[2]);
    // The port that this server should listen on to accept connections from other servers
    int serverListenPort = Integer.parseInt(args[1]);
    try {
      //create a coordinator server, requiring the database name and the number of servers
      CoordinatorNode coordinator = new CoordinatorNode(args[4], Integer.parseInt(args[5]));

      coordinator.acceptServers(serverListenPort);
      coordinator.acceptClients(clientListenPort);

      //wait while the protocol is running.

    }
    catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void setUpSlave(String[] args) {

    InetSocketAddress[] coordinatorAddress = Server.parseAddresses(args[3]);
    try {

      SlaveNode slave = new SlaveNode(args[4]);

      slave.connectServers(coordinatorAddress);


    } catch (Exception e){
      System.out.println(e);
    }

  }

}

class CoordinatorNode extends Server {

  //enums for states in the protocol
  private static final int QUERY = 1;
  private static final int ABORT = 0;
  private static final int PRECOMMIT = 2;
  private static final int COMMIT = 3;


  //All connection logic is handled int he ThreadedCoordinatorWorker, 1 needed per connection
  private ThreadedCoordinatorWorker[] slaveConnections;
  //threadpool to limit number of threads concurrently executing
  // Code samples I consulted in learning the correct useage of threadpools is here:
  // http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html
  private ExecutorService threadpool;
  // completion service to allow callable interface
  // As I am using callable to read return values from threads, I needed a completion service to get the
  // result from a thread without interupting it. I consulted stack overflow for this:
  // http://stackoverflow.com/questions/4912228/when-should-i-use-a-completionservice-over-an-executorservice
  private CompletionService<ReturnMessage<String>> completionService;
  private ServerSocket serverSocket;
  // counter used to check responses from all servers
  private AtomicInteger counter;
  private int numOfSlaves;
  private boolean acceptingFlag = false;
  private boolean connectedToSlaves = false;
  private Thread slaveListenerThread;

  public CoordinatorNode(String databasePath, int n) throws IOException {
    super(databasePath);
    // 16 threads allowed at once, 4 per core presuming a 4 core system, which the DCS machine i tested on had.
    this.threadpool = Executors.newFixedThreadPool(16);
    this.completionService = new ExecutorCompletionService<ReturnMessage<String>>(threadpool);
    this.counter = new AtomicInteger(0);
    this.numOfSlaves = n;
    this.slaveConnections = new ThreadedCoordinatorWorker[numOfSlaves];
  }

  @Override
  public void acceptServers(int port) throws IOException {
    serverSocket = new ServerSocket(port);
    //set the SO timeout to 10000, so if a coordinator is not running the coordinator will abort the connection
    serverSocket.setSoTimeout(10000);
    acceptingFlag = true;


    // Run the connections in a new thread so that the server is able to take a client request
    (slaveListenerThread = new Thread(() -> {
      Socket conn;
      int i = 0;
      boolean timeout = false;
      //timers to control the number of connection attemps.


      //try and make a connection for each slave. if there is a timout, exit the loop
      while (i < numOfSlaves) {

        try {
          conn = serverSocket.accept();
          synchronized (this.slaveConnections) {
            this.slaveConnections[i] = new ThreadedCoordinatorWorker(conn, i);
          }
          i++;

        } catch (Exception e) {
          e.printStackTrace();
          timeout = true;
          break;
        }
      }
      acceptingFlag = false;
      if (!timeout) connectedToSlaves = true;
      else connectedToSlaves = false;
      //abort the coordinator setup and close if unable to connect to the slaves
      if (!connectedToSlaves) {
        try {
          System.out.println("<coordinator> <unable to connect to cohort. Closing>");
          this.close();
        } catch (IOException e){
          e.printStackTrace();
        }
      }
    })).start();

  }

  @Override
  public void connectServers(InetSocketAddress[] servers) throws IOException{
    throw new UnsupportedOperationException("Not supported by this server.");
  }

  // this method will be ran when waiting for all slaves to respond to a message
  public void waitingState(int mode) {
    for (int i = 0; i < numOfSlaves; i++ ){
      try {

        Future<ReturnMessage<String>> futureResponse = completionService.take();
        // Get the contents of the response. Only runs when the thread has finsihed, has a timeout of 10s
        ReturnMessage<String> message = futureResponse.get(10, TimeUnit.SECONDS);

        String messageContent = message.getMessage();
        //case statement to switch on what message was and what mode the coordinator is in
        switch (mode) {
          case QUERY:
            if (messageContent.equals("READY")) counter.incrementAndGet();
            else counter.decrementAndGet();
            break;

          case ABORT:
          case PRECOMMIT:
          case COMMIT:
              if (messageContent.equals("ACK")) counter.incrementAndGet(); break;
        }
      } catch (TimeoutException e) {
        abortState();
      } catch (InterruptedException s) {
        s.printStackTrace();
      } catch (ExecutionException t) {
        t.printStackTrace();
      }

    }

  }

  public void queryState(StockList stock) {
    OutgoingMessage<StockList, Integer> message = new OutgoingMessage<StockList, Integer>(stock, QUERY);
    sendMessages(message);
  }

  public void abortState() {
    System.out.println("<coordinator> <Aborting>");
    OutgoingMessage<StockList, Integer> message = new OutgoingMessage<StockList, Integer>(null, ABORT);
    sendMessages(message);
  }

  public void preCommitState(StockList stock) {
    OutgoingMessage<StockList, Integer> message = new OutgoingMessage<StockList, Integer>(stock, PRECOMMIT);
    sendMessages(message);
  }

  public void commitState(StockList stock) {
    OutgoingMessage<StockList, Integer> message = new OutgoingMessage<StockList, Integer>(stock, COMMIT);
    sendMessages(message);
  }

  public void sendMessages(OutgoingMessage<StockList, Integer> outgoingMessage) {

    // for each ThreadedCoordinatorWorker, give it the outgoing message and add to the threadpool
    for (int i = 0; i < numOfSlaves; i++){
      slaveConnections[i].setOutgoingMessage(outgoingMessage);
      this.completionService.submit(slaveConnections[i]);
    }
  }

  @Override
  public boolean handleClientRequest(StockList stock) throws IOException{
    this.counter.set(0);
    try {
      //wait for connections to the slaves to finish
      while (acceptingFlag) {Thread.sleep(500);}
      //starting phase 1
      StockList coordinatorDatabase = this.queryDatabase();
      if (coordinatorDatabase.enough(stock)) {
        System.out.println("<coordinator> <in query state>");
        //send query messages to the server
        queryState(stock);

        System.out.println("<coordinator> <in query-waiting state>");
        waitingState(QUERY);


        //starting phase 2
        if (counter.get() == numOfSlaves) {
          System.out.println("<coordinator> <all slaves READY>");
          counter.set(0);

          System.out.println("<coordinator> <In precommit state>");
          preCommitState(stock);
          System.out.println("<coordinator> <In precommit-waiting state>");
          waitingState(PRECOMMIT);


          //starting phase 3
          if (counter.get() == numOfSlaves ) {
              System.out.println("<coordinator> <all slaves pre-committed>");

            counter.set(0);
            System.out.println("<coordinator> <In commit state>");
            coordinatorDatabase.remove(stock);
            writeDatabase(coordinatorDatabase);

            commitState(stock);

            System.out.println("<coordinator> <In commit-waiting state>");
            waitingState(COMMIT);
            System.out.println("<coordinator> <all slaves Committed>");
            if (counter.get() == numOfSlaves) {

              return true;
            }
          } else {abortState(); waitingState(ABORT); System.out.println("<coordinator> <timeout on precommit>");return false;}
        } else {abortState(); waitingState(ABORT);System.out.println("<coordinator> <timout or unable on query>");return false;}
      } else {
        System.out.println("<coordinator> <coordinator UNABLE>");
        abortState();
        waitingState(ABORT);
        return false;

      }

      return false;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public void close() throws IOException {

    System.out.println("<coordinator> <Closing>");
    super.close();

    //close the threadpool, ending all running threads.
    threadpool.shutdown();

    //close the connections to all slave servers
    for (int i = 0; i < numOfSlaves; i++){
      if (slaveConnections[i] == null) continue;
      slaveConnections[i].close();
    }
    System.out.println("<coordinator> <Closed>");
  }

}


//Object which contains a socket, IO streams for the socket and methods to message a slave
class ThreadedCoordinatorWorker implements Callable<ReturnMessage<String>> {
  private ReturnMessage<String> incomingMessage;
  private OutgoingMessage<StockList, Integer> outgoingMessage;
  private Socket connection;
  private ObjectOutputStream os;
  private ObjectInputStream is;
  private int threadNo;

  ThreadedCoordinatorWorker(Socket connection, int i) throws IOException{
    // I followed a tutorial on using sockets here:
    // http://www.tutorialspoint.com/java/java_networking.htm

    System.out.println("<coordinator> <Created new thread object, threadno: " + i + ">");
    this.connection = connection;
    threadNo = i;

    this.os = new ObjectOutputStream(connection.getOutputStream());
    this.is = new ObjectInputStream(connection.getInputStream());
  }

  //constructor for creating object to return response from a slave to the coordinator
  ThreadedCoordinatorWorker(ReturnMessage<String> message) {
    this.incomingMessage = message;
  }

  public ReturnMessage<String> getReturnMessage() {
    return this.incomingMessage;
  }

  public void close() throws IOException{
    System.out.println("<thread> <closed connection>");
    this.connection.close();
  }

  public void setOutgoingMessage(OutgoingMessage<StockList, Integer> outgoingMessage) {
    this.outgoingMessage = outgoingMessage;
  }

  // method to write the outgoing message to the output stream and read a response, packaging it into a new ThreadedCoordinatorWorker
  public ReturnMessage<String> call() throws InterruptedException, IOException, ClassNotFoundException {
    System.out.println("<thread> <Running thread>");
    try {

      System.out.println("<thread> <Writing message to " + connection + ">");
      os.writeObject(this.outgoingMessage);
      os.flush();
      // set the socket timout to 10s. At this time, assume the slave failed and throw an exception to abort
      connection.setSoTimeout(10000);
      System.out.println("<thread> <wrote message " + this.outgoingMessage.getMessage() + ">");

      System.out.println("<thread> <waiting for response>");
      incomingMessage = (ReturnMessage<String>) is.readObject();
      System.out.println("<thread> <Got resoponse " + incomingMessage.getMessage() + " from " + connection + ">");
      // set timeout to infinite while waiting for next request to send
      connection.setSoTimeout(0);
      return incomingMessage;


    } catch (Exception e) {
      e.printStackTrace();
      throw new InterruptedException();
    }

  }

}


class SlaveNode extends Server {

  private static final int QUERY = 1;
  private static final int ABORT = 0;
  private static final int PRECOMMIT = 2;
  private static final int COMMIT = 3;

  private Socket connection;
  private ObjectOutputStream os;
  private ObjectInputStream is;
  private boolean isConnected;
  //stage indicates if querying or (pre)committing
  private boolean stage;
  // Lock variable so only one request can access DB at once.
  private boolean isLocked;
  private OutgoingMessage<StockList, Integer> inmessage;
  private ReturnMessage<String> outmessage = null;

  public SlaveNode(String databasePath) throws IOException {
    super(databasePath);
    isConnected = false;
    stage = false;
    isLocked = false;
  }

  public void acceptServers(int port) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
  }

  public ReturnMessage<String> checkStock(StockList stock) throws IOException, InterruptedException {
    try {
      System.out.println("<slave> <In query state>");
      // If locked, another transaction is in progress, so wait
      while (isLocked) Thread.sleep(200);
      System.out.println("<slave> <Locked the database>");
      this.isLocked = true; // lock the database
      this.connection.setSoTimeout(10000); //set the timeout to 10s, so if the coordinator fails no deadlock occurs
      StockList currentStock = this.queryDatabase();
      boolean enough = currentStock.enough(stock);


      if (enough) return new ReturnMessage<String>("READY");
      else return new ReturnMessage<String>("UNABLE");
    } catch (InterruptedException e) {
      throw new InterruptedException();
    }
  }

  public ReturnMessage<String> preCommitChange(){
    System.out.println("<slave> <In precommit state>");
    stage = true; //mark that ready to precommit, so can commit in event of coordinator failure

    return new ReturnMessage<String>("ACK");
  }

  public ReturnMessage<String> commitChange(StockList stock) throws IOException{
    System.out.println("<slave> <In commit state>");
    StockList currentStock = this.queryDatabase();
    currentStock.remove(stock);
    this.writeDatabase(currentStock);
    this.isLocked = false;
    this.stage = false; //set stage to false for next transaction
    this.connection.setSoTimeout(0); //set infinite timout as unknown when the next transaction will be
    System.out.println("<slave> <released the database lock>");
    //release lock
    return new ReturnMessage<String>("ACK");
  }

  public ReturnMessage<String> abortChange() throws SocketException {
    System.out.println("<slave> <In abort state>");
    System.out.println("<slave> <released the database lock>");
    //release lock
    this.isLocked = false;
    this.stage = false; //set state to false for next transaction
    this.connection.setSoTimeout(0); //set infinite timout as unknown when the next transaction will be
    return new ReturnMessage<String>("ACK");

  }

  public void close() throws IOException{
    super.close();
    this.connection.close();
  }

  public void runProtocol() throws IOException {
    try {
      //run while not stopping as there may be multiple client requests
      while (!isStopping()){
        System.out.println("<slave> <waiting for incomming message>");
        inmessage = (OutgoingMessage<StockList, Integer>) is.readObject();

        System.out.println("<slave> <Recieved " + inmessage.getMessage() + " from server>");

        Integer mode = inmessage.getMode();
        // mode indicates current stage of the protocol
        switch (mode) {
          case ABORT: outmessage = abortChange(); break;
          case QUERY: outmessage = checkStock(inmessage.getMessage());break;
          case PRECOMMIT: outmessage = preCommitChange();break;
          case COMMIT: outmessage = commitChange(inmessage.getMessage());break;
        }
        System.out.println("<slave> <sending message to coordinator: " + outmessage.getMessage() + ">");
        os.writeObject(outmessage);

        System.out.println("<slave> <Sent message to coordinator>");
      }

    } catch (SocketTimeoutException s) {
      // if timeout after the query stage, abort everything and quit.
      if (!stage) {
        System.out.println("<slave> <slave timed out. Uncertain if should commit. Aborting...>");
        abortChange();
        System.out.println("<slave> <aborted transaction>");
      } else {
        //if timeout in the precommit or commit stage, commit everything and quit.
        System.out.println("<slave> <slave timed out. Prepared to commit. Committing...>");
        commitChange(inmessage.getMessage());
        System.out.println("<slave> <committed>");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException t) {
      t.printStackTrace();
    }
  }


  @Override
  public void connectServers(InetSocketAddress[] serveraddress) throws IOException {
    // In a while loop so that it can attempt to connect multiple times, in case
    // coordinator is busy when attempting to connect
    int attempts = 100;
    while (!isConnected){
      try{
        System.out.println("<slave> <Creating port to listen to coordinator>");
        int port = serveraddress[0].getPort();
        InetAddress address = serveraddress[0].getAddress();
        //attempt to connect
        this.connection = new Socket(address, port);
        //set timeout to infinite while servers are constructed.
        connection.setSoTimeout(0);
        System.out.println("<slave> <Established listening port>");

        System.out.println("<slave> <Creating in/out streams>");
        os = new ObjectOutputStream(this.connection.getOutputStream());
        is = new ObjectInputStream(this.connection.getInputStream());
        System.out.println("<slave> <Created in/out streams>");
        isConnected = true;



      } catch (ConnectException e) {
        // if the connection failed, wait then try again
        try {
          Thread.sleep(100);
        } catch (InterruptedException f) {
          f.printStackTrace();
        }
        //if 100 attempts are made, timout and stop attempting to connect. Throw IOException
        attempts -= 1;
        if (attempts >= 1) continue;
        System.out.println("<slave> <unable to connect to the coordinator>");
        throw new IOException();
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }

      if (isConnected) runProtocol();
      //protocol handling
    }
  }

  @Override
  public boolean handleClientRequest(StockList stock) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
  }
}


// Return message object to package a response for stream transport
class ReturnMessage<E> implements Serializable {
  private E message;

  ReturnMessage(E message) {
    this.message = message;
  }

  public E getMessage() {
    return this.message;
  }
}

// outgoing message object to package a request and mode for stream transport
class OutgoingMessage<E, V> implements Serializable {
  private E message;
  private V mode;

  OutgoingMessage(E message) {
    this.message = message;
  }

  OutgoingMessage(E message, V mode) {
    this.message = message;
    this.mode = mode;
  }

  public E getMessage() {
    return this.message;
  }
  public V getMode() {
    return this.mode;
  }
}
