import HDFS.hdfs;
import com.google.protobuf.InvalidProtocolBufferException;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Jobtracker implements Jobtrackerdef {
    int num_jobs;
    HashMap<Integer, jobstatus> jobs; /* map containing the job status is done */
    Queue<Integer> map_queue; /* Job Queue */
    Queue<Integer> reduce_queue; /* Job Queue */
    static Namenodedef namenode_stub;

    public Jobtracker() {
        num_jobs = 0;
        jobs = new HashMap<>();
        map_queue = new LinkedList<>();
        reduce_queue = new LinkedList<>();
    }

    public byte[] jobSubmit(byte[] inp) {
        /* need to get all the block location info and add it to the jobstatus */
        try {
            num_jobs++;
            hdfs.JobSubmitRequest request = hdfs.JobSubmitRequest.parseFrom(inp);
            hdfs.JobSubmitResponse.Builder response = hdfs.JobSubmitResponse.newBuilder();
            String file_name = request.getInputFile();
            String mapper = request.getMapName();
            String reducer = request.getReducerName();
            int num_reducers = request.getNumReduceTasks();
            hdfs.OpenFileRequest.Builder openFileRequest = hdfs.OpenFileRequest.newBuilder();
            openFileRequest.setForRead(true);
            openFileRequest.setFileName(file_name);
            byte[] open_resp = namenode_stub.openFile(openFileRequest.build().toByteArray());
            if (open_resp != null) {
                hdfs.OpenFileResponse resp = hdfs.OpenFileResponse.parseFrom(open_resp);
                int block_count = resp.getBlockNumsCount();
                jobstatus new_job = new jobstatus(block_count, num_reducers, mapper, reducer);
                /* Get the Block Locations for all */
                hdfs.BlockLocationRequest.Builder blockLocationRequest = hdfs.BlockLocationRequest.newBuilder();
                blockLocationRequest.addAllBlockNums(resp.getBlockNumsList());
                System.err.println(resp.getBlockNumsList());
                byte[] resp_bytes = namenode_stub.getBlockLocations(blockLocationRequest.build().toByteArray());
                if (resp_bytes != null) {
                    /* get the blocks and assign each block a map_info in map_status */
                    hdfs.BlockLocationResponse location_resp = hdfs.BlockLocationResponse.parseFrom(resp_bytes);
                    for (int i = 0; i < block_count; i++) {
                        hdfs.BlockLocations loc = location_resp.getBlockLocations(i);
                        map_info task_info = new map_info(loc);
                        new_job.map_status.put(i, task_info);
                    }
                    jobs.put(num_jobs, new_job);
                    response.setStatus(0); /* Just started */
                    response.setJobId(num_jobs);
                    return response.build().toByteArray();
                }
            }
        } catch (InvalidProtocolBufferException | RemoteException e) {
            e.printStackTrace();
        }
        return null;
    }


    public byte[] getJobStatus(byte[] inp) {
        /* Just check all the reduce tasks and map tasks and return correspodingly */
        try {
            hdfs.JobStatusRequest request = hdfs.JobStatusRequest.parseFrom(inp);
            hdfs.JobStatusResponse.Builder response = hdfs.JobStatusResponse.newBuilder();
            int job_id = request.getJobId();
            jobstatus status = jobs.get(job_id);
            if (!status.status) {
                boolean is_done = true;
                for (int i = 1; i <= status.total_reduce; i++) {
                    if (status.reduce_status.get(i) == null || !status.reduce_status.get(i).status) {
                        is_done = false;
                        break;
                    }
                }
                if (is_done) {
                        /* Set the status to done */
                    status.status = true;
                    response.setStatus(1);
                    response.setTotalMapTasks(status.total_map);
                    response.setTotalReduceTasks(status.total_reduce);
                    response.setNumMapTasksStarted(status.started_map);
                    response.setTotalReduceTasks(status.started_reduce);
                } else {
                    response.setStatus(0);

                }

            } else {
                response.setStatus(1);
            }
            return response.build().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    static void assign_reducers(jobstatus job_status) {
        if (job_status.total_reduce < job_status.total_map) {
            /* Caluclate the number of maps needed for reducers */
            int factor = job_status.total_map / job_status.total_reduce + 1;
            /* Factor into chunks of this factor and assign to reducers */
            int rem = job_status.total_map;
            int sofar = 1;
            while (sofar < rem) {
                reduce_info info = new reduce_info();
                for (int i = 1; i <= factor && sofar < rem; i++) {
                    info.add_string(job_status.map_status.get(sofar).output_file);
                    sofar++;
                }
            }
        } else {
            for (int i = 1; i <= job_status.total_map; i++) {
                reduce_info info = new reduce_info();
                info.add_string(job_status.map_status.get(i).output_file);
                job_status.reduce_status.put(i, info);
            }
        }
    }

    public byte[] heartBeat(byte[] inp) {
        try {
            hdfs.HeartBeatRequestMapReduce request = hdfs.HeartBeatRequestMapReduce.parseFrom(inp);
            hdfs.HeartBeatResponseMapReduce.Builder response = hdfs.HeartBeatResponseMapReduce.newBuilder();
            int map_slots = request.getNumMapSlotsFree();
            int reduce_slots = request.getNumReduceSlotsFree();
            if (map_queue.size() > 0) {
                int job = map_queue.element();
                jobstatus status = jobs.get(job);
                while (status.started_map < status.total_map && map_slots > 0) {
                    /* There are map tasks pending so assign them*/
                    status.started_map++;
                    int task_id = status.started_map;
                    System.err.println("Assigned map");
                    hdfs.MapTaskInfo.Builder task = hdfs.MapTaskInfo.newBuilder();
                    task.setInputBlocks(status.map_status.get(task_id).loc_info);
                    task.setJobId(job);
                    task.setTaskId(task_id);
                    task.setMapName(status.mapname);
                    map_slots--;
                }
                if (status.started_map == status.total_map) {
                    /* All are assigned so remove it from queue */
                    map_queue.remove();
                }
                jobs.put(job, status);
            }
            if (reduce_queue.size() > 0) {
                int job = map_queue.element();
                jobstatus status = jobs.get(job);
                while (status.started_reduce < status.total_reduce && reduce_slots > 0) {
                    /* There are reduce tasks pending so assign them*/
                    status.started_reduce++;
                    int task_id = status.started_reduce;
                    hdfs.ReducerTaskInfo.Builder task = hdfs.ReducerTaskInfo.newBuilder();
                    task.setJobId(job);
                    task.setTaskId(task_id);
                    System.err.println("Assigned Reduced");
                    task.addAllMapOutputFiles(status.reduce_status.get(task_id).output_files);
                    reduce_slots--;
                }
                jobs.put(job, status);
                if (status.started_reduce == status.total_reduce) {
                    /* All are assigned so remove it from queue */
                    reduce_queue.remove();
                }
            }
            List<hdfs.MapTaskStatus> map_statuses = request.getMapStatusList();
            /* If the task is done then do something*/
            map_statuses.stream().filter(hdfs.MapTaskStatus::getTaskCompleted).forEach(status -> {
                    /* If the task is done then do something*/
                int job_id = status.getJobId();
                int task_id = status.getTaskId();
                jobstatus job_status = jobs.get(job_id);
                map_info task_status = job_status.map_status.get(task_id);
                task_status.status = true;
                task_status.output_file = status.getMapOutputFile();
                /* Check whether the given job is done if it is then add it to reducer queue and process reduce */
                boolean is_done = true;
                for (int i = 1; i <= job_status.total_map; i++) {
                    if (!job_status.map_status.get(i).status) {
                        is_done = false;
                        break;
                    }
                }
                if (is_done) {
                    assign_reducers(job_status);
                }
            });
            List<hdfs.ReduceTaskStatus> reduce_statuses = request.getReduceStatusList();
            reduce_statuses.stream().filter(hdfs.ReduceTaskStatus::getTaskCompleted).forEach(status -> {
                /* If the task is done then do something*/
                int job_id = status.getJobId();
                int task_id = status.getTaskId();
                jobstatus job_status = jobs.get(job_id);
                reduce_info task_status = job_status.reduce_status.get(task_id);
                task_status.status = true;
            });
            return response.build().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {

        Jobtracker job_tracker = new Jobtracker();
        try {
            System.setProperty("java.rmi.server.hostname", "10.1.39.155");
            Jobtrackerdef stub = (Jobtrackerdef) UnicastRemoteObject.exportObject(job_tracker, 0);
            Registry reg = LocateRegistry.getRegistry("0.0.0.0", 1099);
            reg.rebind("JobTracker", stub);
            String namenode_host = "10.1.39.155";
            int namenode_ip = 1099;
            Registry registry = LocateRegistry.getRegistry(namenode_host, namenode_ip);
            namenode_stub = (Namenodedef) registry.lookup("NameNode");

        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
    }
}

class jobstatus {
    /* Class to store the info about a job */
    boolean status;
    int total_map;
    int total_reduce;
    int started_map;
    int started_reduce;
    String mapname;
    String reducename;
    HashMap<Integer, map_info> map_status;
    HashMap<Integer, reduce_info> reduce_status;

    jobstatus(int map_total, int reduce_total, String mapper, String reducer) {
        mapname = mapper;
        reducename = reducer;
        status = false;
        total_map = map_total;
        total_reduce = reduce_total;
        started_map = 0;
        started_reduce = 0;
        map_status = new HashMap<>();
        reduce_status = new HashMap<>();
    }
}

class map_info {
    /* Store the info for a map task */
    boolean status;
    hdfs.BlockLocations loc_info;
    String output_file;

    map_info(hdfs.BlockLocations loc) {
        status = false;
        loc_info = loc;
    }
}

class reduce_info {
    /* Store the info for a Reduce task */
    boolean status;
    List<String> output_files;

    reduce_info() {
        status = false;
        output_files = new ArrayList<>();
    }

    void add_string(String output_file) {
        output_files.add(output_file);
    }
}