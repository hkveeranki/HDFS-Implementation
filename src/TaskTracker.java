import HDFS.hdfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TaskTracker {
    private static Jobtrackerdef jobtracker_stub;
    private static int map_capacity = 2;
    private static int reduce_capacity = 2;
    private static ThreadPoolExecutor map_pool;
    private static ThreadPoolExecutor reduce_pool;

    public TaskTracker() {
    }

    static public void main(String args[]) {
        try {
            String host = "127.0.0.1";
            int port = 1099;
            map_pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(map_capacity);
            reduce_pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(reduce_capacity);
            Registry registry = LocateRegistry.getRegistry(host, port);
            jobtracker_stub = (Jobtrackerdef) registry.lookup("JobTracker");
            int id = Integer.valueOf(args[0]);
            new HeartbeatHandler(id).run();
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }

    static private void map_executor(byte[] info) {
        /* this method performs the map task assigned */


    }

    static private void reduce_executor(byte[] info) {
        /* this method performs the reduce task assigned */
    }

    private static class HeartbeatHandler extends Thread {
        /* Handles the heart beat */
        private int id;

        HeartbeatHandler(int node_id) {
            id = node_id;
        }

        public void run() {
            try {
                while (true) {
                /* Send Periodically HeartBeat */
                    hdfs.HeartBeatRequestMapReduce.Builder request = hdfs.HeartBeatRequestMapReduce.newBuilder();
                    request.setTaskTrackerId(id);
                    request.setNumMapSlotsFree(map_capacity - map_pool.getActiveCount());
                    request.setNumReduceSlotsFree(reduce_capacity - reduce_pool.getActiveCount());
                    /* Need to set the status as well */
                    jobtracker_stub.heartBeat(request.build().toByteArray());
                    System.err.println("Sent HeartBeat from Task Tracker " + id);
                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
