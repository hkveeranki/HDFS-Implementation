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
    private static HashMap<String, hdfs.MapTaskStatus> map_statuses;
    private static HashMap<String, hdfs.ReduceTaskStatus> reduce_statuses;

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
        /* Each thread from the map_pool runs this function */
        hdfs.MapTaskInfo map_info = hdfs.MapTaskInfo.parseFrom(info);
        List<hdfs.BlockLocations> block_locs = map_info.getInputBlocksList();
        int jobId = map_info.getJobId();
        int taskId = map_info.getTaskId();
        String out_file = "map_" + map_info.getJobId().toString() + "_" + map_info.getTaskId().toString();
        for(int j = 0; j < block_locs.size(); j++){
            /* Randomly select a datanode for the given block to use */
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
                String mapName = map_info.getMapName();
                Class mapper = Class.forName(mapName); // Is this correct?

            }
        }
        hdfs.MapTaskStatus.Builder map_stat = hdfs.MapTaskStatus.newBuilder();
        map_stat.setJobId(map_info.getJobId());
        map_stat.setTaskId(map_info.getTaskId());
        map_stat.setTaskCompleted(true);
        map_stat.setMapOutputFile(out_file);
        map_statuses.put(out_file, map_stat.build());
    }

    static private void reduce_executor(byte[] info) {
        /* this method performs the reduce task assigned */
        /* Each thread from the reduce_pool runs this function */
        hdfs.ReducerTaskInfo reduce_info = hdfs.ReducerTaskInfo.parseFrom(info);
        List<String> map_output_files = reduce_info.getMapOutputFilesList();
        String out_file = reduce_info.getOutputFile();
        int jobId = map_info.getJobId();
        int taskId = map_info.getTaskId();
        String idx = "reduce_" + jobId.toString() + "_" + taskId.toString();
        for(int j = 0; j < map_output_files.size(); j++){
            String reducerName = reduce_info.getReducerName();
            Class reducer = Class.forName(reducerName); // Is this correct?

        }
        hdfs.ReduceTaskStatus.Builder reduce_stat = hdfs.ReduceTaskStatus.newBuilder();
        reduce_stat.setJobId(jobId);
        reduce_stat.setTaskId(taskId);
        reduce_stat.setTaskCompleted(true);
        reduce_statuses.put(idx, reduce_stat.build());
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
                    for (HashMap.Entry<String, hdfs.MapTaskStatus> entry : map_statuses.entrySet()) {
                        String key = entry.getKey();
                        hdfs.MapTaskStatus map_status = entry.getValue();
                        request.addMapStatus(map_status);
                        if(map_status.getTaskCompleted() == true){
                            map_statuses.remove(key);
                        }
                    }
                    for (HashMap.Entry<String, hdfs.ReduceTaskStatus> entry : reduce_statuses.entrySet()) {
                        String key = entry.getKey();
                        hdfs.ReduceTaskStatus reduce_status = entry.getValue();
                        request.addReduceStatus(reduce_status);
                        if(reduce_statuses.getTaskCompleted() == true){
                            reduce_statuses.remove(key);
                        }
                    }
                    /* Get response and act on it */
                    byte[] resp = jobtracker_stub.heartBeat(request.build().toByteArray());
                    System.err.println("Sent HeartBeat from Task Tracker " + id);
                    hdfs.HeartBeatResponseMapReduce.Builder response = hdfs.HeartBeatResponseMapReduce.parseFrom(resp);
                    List<hdfs.MapTaskInfo> map_infos response.getMapTasksList();
                    List<hdfs.ReducerTaskInfo> reduce_infos response.getReduceTasksList();
                    for (int i = 0; i < map_infos.size(); i++){
                        hdfs.MapTaskInfo map_info = map_infos.get(i);
                        hdfs.MapTaskStatus.Builder map_stat = hdfs.MapTaskStatus.newBuilder();
                        map_stat.setJobId(map_info.getJobId());
                        map_stat.setTaskId(map_info.getTaskId());
                        map_stat.setTaskCompleted(false);
                        String out_file = "map_" + map_info.getJobId().toString() + "_" + map_info.getTaskId().toString();
                        map_stat.setMapOutputFile(out_file);
                        map_statuses.put(out_file, map_stat.build());
                        map_executor(map_info.toByteArray());
                    }
                    for (int i = 0; i < reduce_infos.size(); i++){
                        hdfs.ReduceTaskInfo reduce_info = reduce_infos.get(i);
                        hdfs.ReduceTaskStatus.Builder reduce_stat = hdfs.ReduceTaskStatus.newBuilder();
                        reduce_stat.setJobId(reduce_info.getJobId());
                        reduce_stat.setTaskId(reduce_info.getTaskId());
                        reduce_stat.setTaskCompleted(false);
                        String idx = "reduce" + reduce_info.getJobId().toString() + "_" + reduce_info.getTaskId().toString();
                        reduce_statuses.put(idx, reduce_stat.build());
                        reduce_executor(reduce_info.toByteArray());
                    }
                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
