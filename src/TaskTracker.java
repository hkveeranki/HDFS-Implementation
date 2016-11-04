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
    static Namenodedef namenode_stub;

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
        hdfs.MapExecutorRequest.Builder request = hdfs.MapExecutorRequest.parseFrom(info);
        hdfs.MapTaskInfo map_info = request.getMapInfo();
        List<hdfs.BlockLocations> block_locs = map_info.getInputBlocksList();
        for(int j = 0; j < block_locs.size(); j++){
            DataNodeLocation dnLoc = block_locs[j].getLocationsList()[0];
            int blockNum = block_locs[j].getBlockNumber();
            Registry registry = LocateRegistry.getRegistry(dnLoc.getIp(), dnLoc.getPort());
            Datanodedef datanode_stub = (Datanodedef) registry.lookup("DataNode");
            hdfs.ReadBlockRequest.Builder read_req = hdfs.ReadBlockRequest.newBuilder();
            read_req.setBlockNumber(blockNum);
            byte[] read_resp = datanode_stub.readBlock(read_req.build().toByteArray());
            if (read_resp != null) {
                hdfs.ReadBlockResponse readBlockResponse = hdfs.ReadBlockResponse.parseFrom(read_resp);
                ByteString data = readBlockResponse.getData(0);
                int jobId = map_info.getJobId();
                int taskId = map_info.getTaskId();
                String mapName = map_info.getMapName();
                Class mapper = Class.forName(mapName); // Is this correct?
                // Use the thread pool to execute this task here
            }
        }
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

                    byte[] resp = jobtracker_stub.heartBeat(request.build().toByteArray());
                    System.err.println("Sent HeartBeat from Task Tracker " + id);
                    hdfs.HeartBeatResponseMapReduce.Builder response = hdfs.HeartBeatResponseMapReduce.parseFrom(resp);
                    List<hdfs.MapTaskInfo> map_infos response.getMapTasksList();
                    for (int i = 0; i < map_infos.size(); i++){
                        hdfs.MapTaskInfo map_info = map_infos.get(i);
                        hdfs.MapExecutorRequest.Builder map_exec_request = hdfs.MapExecutorRequest.newBuilder();
                        map_exec_request.setMapInfo(map_info);
                        map_executor(map_exec_request.build().toByteArray());
                    }

                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
