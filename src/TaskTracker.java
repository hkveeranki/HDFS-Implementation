import HDFS.hdfs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
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
    private static Helper helper;

    public TaskTracker() {
    }

    static public void main(String args[]) {
        try {
            String host = "127.0.0.1";
            String namenode_host = "127.0.0.1";
            int namenode_port = 1099;
            int port = 1099;
            map_pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(map_capacity);
            reduce_pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(reduce_capacity);
            Registry registry = LocateRegistry.getRegistry(host, port);
            jobtracker_stub = (Jobtrackerdef) registry.lookup("JobTracker");
            Registry reg = LocateRegistry.getRegistry(namenode_host, namenode_port);
            namenode_stub = (Namenodedef) reg.lookup("NameNode");
            int id = Integer.valueOf(args[0]);
            map_statuses = new HashMap<>();
            reduce_statuses = new HashMap<>();
            helper = new Helper(namenode_stub);
            new HeartbeatHandler(id).run();
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }

    static private void map_executor(byte[] info) {
        /* this method performs the map task assigned */
        /* Each thread from the map_pool runs this function */
        try {
            hdfs.MapTaskInfo map_info = hdfs.MapTaskInfo.parseFrom(info);
            hdfs.BlockLocations block_loc = map_info.getInputBlocks();
            int jobId = map_info.getJobId();
            int taskId = map_info.getTaskId();
            String out_file = "map_" + Integer.toString(jobId) + "_" + Integer.toString(taskId);
            hdfs.DataNodeLocation dnLoc = block_loc.getLocationsList().get(0);
            int blockNum = block_loc.getBlockNumber();
            Registry registry = LocateRegistry.getRegistry(dnLoc.getIp(), dnLoc.getPort());
            Datanodedef datanode_stub = (Datanodedef) registry.lookup("DataNode");
            hdfs.ReadBlockRequest.Builder read_req = hdfs.ReadBlockRequest.newBuilder();
            read_req.setBlockNumber(blockNum);
            byte[] read_resp = datanode_stub.readBlock(read_req.build().toByteArray());
            if (read_resp != null) {
                String mapName = map_info.getMapName();
                Mapper mymap = (Mapper) Class.forName(mapName).newInstance();
                hdfs.ReadBlockResponse readBlockResponse = hdfs.ReadBlockResponse.parseFrom(read_resp);
                ByteString data = readBlockResponse.getData(0);
                String input = data.toString();
                String out_data = "";
                Scanner scanner = new Scanner(input);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    out_data += mymap.map(line);
                }
                scanner.close();
                if (helper.write_to_hdfs(out_file, out_data)) {
                    /* Set the status only when write is successfull */
                    hdfs.MapTaskStatus.Builder map_stat = map_statuses.get(out_file).toBuilder();
                    map_stat.setTaskCompleted(true);
                    map_statuses.put(out_file, map_stat.build());
                    System.out.println(map_statuses.get(out_file).getTaskCompleted());// Testing Remove after confirmed
                }
            }
        } catch (InvalidProtocolBufferException | ClassNotFoundException | NotBoundException | RemoteException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    static private void reduce_executor(byte[] info) {
        /* this method performs the reduce task assigned */
        /* Each thread from the reduce_pool runs this function */
        try {
            hdfs.ReducerTaskInfo reduce_info = hdfs.ReducerTaskInfo.parseFrom(info);
            List<String> map_output_files = reduce_info.getMapOutputFilesList();
            String out_file = reduce_info.getOutputFile();
            int jobId = reduce_info.getJobId();
            int taskId = reduce_info.getTaskId();
            String idx = "reduce_" + Integer.toString(jobId) + "_" + Integer.toString(taskId);
            String out_data = "";
            for (String map_output_file : map_output_files) {
                String reducerName = reduce_info.getReducerName();
                Reducer reducer = (Reducer) Class.forName(reducerName).newInstance();
                String input = helper.read_from_hdfs(map_output_file);
                Scanner scanner = new Scanner(input);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    out_data += reducer.reduce(line);
                }
                scanner.close();
            }
            if (helper.write_to_hdfs(out_file, out_data)) {
                /* Set the status only when write is successfull */
                hdfs.ReduceTaskStatus.Builder reduce_stat = hdfs.ReduceTaskStatus.newBuilder();
                reduce_stat.setJobId(jobId);
                reduce_stat.setTaskId(taskId);
                reduce_stat.setTaskCompleted(true);
                reduce_statuses.put(idx, reduce_stat.build());
            }
        } catch (InvalidProtocolBufferException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
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
                        if (map_status.getTaskCompleted()) {
                            map_statuses.remove(key);
                        }
                    }
                    for (HashMap.Entry<String, hdfs.ReduceTaskStatus> entry : reduce_statuses.entrySet()) {
                        String key = entry.getKey();
                        hdfs.ReduceTaskStatus reduce_status = entry.getValue();
                        request.addReduceStatus(reduce_status);
                        if (reduce_status.getTaskCompleted()) {
                            reduce_statuses.remove(key);
                        }
                    }
                    /* Get response and act on it */
                    byte[] resp = jobtracker_stub.heartBeat(request.build().toByteArray());
                    System.err.println("Sent HeartBeat from Task Tracker " + id);
                    hdfs.HeartBeatResponseMapReduce response = hdfs.HeartBeatResponseMapReduce.parseFrom(resp);
                    List<hdfs.MapTaskInfo> map_infos = response.getMapTasksList();
                    List<hdfs.ReducerTaskInfo> reduce_infos = response.getReduceTasksList();
                    for (hdfs.MapTaskInfo map_info : map_infos) {
                        hdfs.MapTaskStatus.Builder map_stat = hdfs.MapTaskStatus.newBuilder();
                        map_stat.setJobId(map_info.getJobId());
                        map_stat.setTaskId(map_info.getTaskId());
                        map_stat.setTaskCompleted(false);
                        String out_file = "map_" + String.valueOf(map_info.getJobId()) + "_" + String.valueOf(map_info.getTaskId());
                        map_stat.setMapOutputFile(out_file);
                        map_statuses.put(out_file, map_stat.build());
                        map_executor(map_info.toByteArray());
                    }
                    for (hdfs.ReducerTaskInfo reduce_info : reduce_infos) {
                        hdfs.ReduceTaskStatus.Builder reduce_stat = hdfs.ReduceTaskStatus.newBuilder();
                        reduce_stat.setJobId(reduce_info.getJobId());
                        reduce_stat.setTaskId(reduce_info.getTaskId());
                        reduce_stat.setTaskCompleted(false);
                        String idx = "reduce" + String.valueOf(reduce_info.getJobId()) + "_" +
                                String.valueOf(reduce_info.getTaskId());
                        reduce_statuses.put(idx, reduce_stat.build());
                        reduce_executor(reduce_info.toByteArray());
                    }
                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                }
            } catch (InterruptedException | InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
}
