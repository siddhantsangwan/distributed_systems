import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.zookeeper.Watcher.Event.EventType.None;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    // assume the client disconnected after this time
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Main thread resuming, disconnected from Zookeeper");
        Thread.sleep(1000);
        System.out.println("Exiting application");
    }
    public void connectToZookeeper() throws IOException {
        // this object watches for events, so it has to implement the Watcher interface
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }
    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace("/election/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the leader");
            return;
        }
        System.out.println("I am not the leader, " + smallestChild + " is the leader");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }
    public void close() throws InterruptedException {
        this.zooKeeper.close();
    }
    // what to do when an event occurs
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == None) {
            // SyncConnected means we've successfully connected to ZooKeeper
            if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("Successfully connected to Zookeeper");
            }
            else {
                synchronized (zooKeeper) {
                    System.out.println("Disconnect from Zookeeper event received");
                    zooKeeper.notifyAll();
                }
            }
        }
    }
}
