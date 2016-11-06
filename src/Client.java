import HDFS.hdfs;
import com.google.protobuf.ByteString;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) throws RemoteException, NotBoundException {
        String namenode_ip = "127.0.0.1";
        String jobtracker_ip = "127.0.0.1";
        int block_size = 16384; /* 16 KB */
        Registry reg = LocateRegistry.getRegistry(namenode_ip);
        Registry reg2 = LocateRegistry.getRegistry(jobtracker_ip);
        Namenodedef namenode_stub = (Namenodedef) reg.lookup("NameNode");
        Jobtrackerdef jobtracker_stub = (Jobtrackerdef) reg2.lookup("JobTracker");
        Scanner in = new Scanner(System.in);
        Helper helper = new Helper(namenode_stub);
        String command, file_name;
        PrintStream err = new PrintStream(System.err);
        label:
        for (; ; ) {
            command = in.next();
            switch (command) {
                case "put":
                    /* Write the stuff. */
                    file_name = in.next();
                    hdfs.OpenFileRequest.Builder request = hdfs.OpenFileRequest.newBuilder();
                    request.setForRead(false);
                    request.setFileName(file_name);
                    try {
                        byte[] inp = namenode_stub.openFile(request.build().toByteArray());
                        if (inp != null) {
                            hdfs.OpenFileResponse response = hdfs.OpenFileResponse.parseFrom(inp);
                            int handle = response.getHandle();
                            hdfs.AssignBlockRequest.Builder assignBlockRequest = hdfs.AssignBlockRequest.newBuilder();
                            assignBlockRequest.setHandle(handle);
                            File file = new File(file_name);
                            int file_size = (int) file.length(), read_size = block_size, bytes_read;
                            FileInputStream input = new FileInputStream(file);
                            while (file_size > 0) {
                                if (file_size <= block_size) {
                                    read_size = file_size;
                                }
                                byte[] read_bytes = new byte[file_size];
                                bytes_read = input.read(read_bytes, 0, read_size);
                                file_size -= bytes_read;
                                assert (bytes_read == read_bytes.length);
                                byte[] resp = namenode_stub.assignBlock(assignBlockRequest.build().toByteArray());
                                hdfs.AssignBlockResponse blockResponse = hdfs.AssignBlockResponse.parseFrom(resp);
                                hdfs.BlockLocations loc = blockResponse.getNewBlock();
                                reg = LocateRegistry.getRegistry(loc.getLocations(0).getIp(), loc.getLocations(0).getPort());
                                Datanodedef datanode_stub = (Datanodedef) reg.lookup("DataNode");
                                hdfs.WriteBlockRequest.Builder writeBlockRequest = hdfs.WriteBlockRequest.newBuilder().setReplicate(true);
                                writeBlockRequest.addData(ByteString.copyFrom(Arrays.copyOfRange(read_bytes, 0, bytes_read)));
                                writeBlockRequest.setBlockInfo(loc);
                                resp = datanode_stub.writeBlock(writeBlockRequest.build().toByteArray());
                                if (resp != null) err.println("Write Block Successful");
                                else {
                                    err.println("Write Block at " + loc.getLocations(0).getIp() + " failed");
                                }
                                Arrays.fill(read_bytes, (byte) 0);
                            }
                            hdfs.CloseFileRequest.Builder closeFileRequest = hdfs.CloseFileRequest.newBuilder();
                            closeFileRequest.setHandle(response.getHandle());
                            namenode_stub.closeFile(closeFileRequest.build().toByteArray());
                        } else {
                            err.println("OpenFile Request failed at NameNode: " + namenode_ip);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "get":
                    /* Get The Stuff */
                    file_name = in.next();
                    hdfs.OpenFileRequest.Builder openFileRequest = hdfs.OpenFileRequest.newBuilder();
                    openFileRequest.setForRead(true);
                    openFileRequest.setFileName(file_name);
                    try {
                        byte[] openFileResponseBytes = namenode_stub.openFile(openFileRequest.build().toByteArray());
                        if (openFileResponseBytes != null) {
                            FileOutputStream outputStream = new FileOutputStream(new File(file_name));
                            hdfs.OpenFileResponse response = hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);
                            int block_count = response.getBlockNumsCount();
                            hdfs.BlockLocationRequest.Builder blockLocationRequest = hdfs.BlockLocationRequest.newBuilder();
                            blockLocationRequest.addAllBlockNums(response.getBlockNumsList());
                            byte[] resp_bytes = namenode_stub.getBlockLocations(blockLocationRequest.build().toByteArray());

                            if (resp_bytes != null) {
                                Random generator = new Random();
                                hdfs.BlockLocationResponse resp = hdfs.BlockLocationResponse.parseFrom(resp_bytes);
                                List<Integer> blocks = response.getBlockNumsList();
                                for (int i = 0; i < block_count; i++) {
                                    hdfs.BlockLocations loc = resp.getBlockLocations(i);
                                    int loc_ind = generator.nextInt(2);
                                    hdfs.DataNodeLocation dnLocation = loc.getLocations(loc_ind);
                                    Registry registry = LocateRegistry.getRegistry(dnLocation.getIp(), dnLocation.getPort());
                                    Datanodedef datanode_stub = (Datanodedef) registry.lookup("DataNode");
                                    hdfs.ReadBlockRequest.Builder read_req = hdfs.ReadBlockRequest.newBuilder();
                                    read_req.setBlockNumber(blocks.get(i));
                                    byte[] read_resp = datanode_stub.readBlock(read_req.build().toByteArray());
                                    if (read_resp != null) {
                                        hdfs.ReadBlockResponse readBlockResponse = hdfs.ReadBlockResponse.parseFrom(read_resp);
                                        ByteString data = readBlockResponse.getData(0);
                                        outputStream.write(data.toByteArray());
                                    } else {
                                        err.println("Error Getting read from DataNode: " + dnLocation.getIp());
                                    }
                                }
                            } else {
                                err.println("Unable to get the Block Locations");
                            }
                            outputStream.close();
                            err.println("Get file Successfull");
                        } else {
                            err.println("OpenFile Request failed at NameNode: " + namenode_ip);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "list":
                /* List the Stuff */
                    try {
                        byte[] response = namenode_stub.list(null);
                        hdfs.ListFilesResponse listfile_response = hdfs.ListFilesResponse.parseFrom(response);
                        listfile_response.getFileNamesList().forEach(err::println);
                        err.println("list done");
                    } catch (Exception ignored) {

                    }
                    break;
                case "job":
                    String command = in.nextLine();
                    String[] params = command.split(" ");
                    hdfs.JobSubmitRequest.Builder job_request = hdfs.JobSubmitRequest.newBuilder();
                    job_request.setMapName(params[0]);
                    job_request.setReducerName(params[1]);
                    job_request.setInputFile(params[2]);
                    job_request.setOutputFile(params[3]);
                    job_request.setNumReduceTasks(Integer.valueOf(params[4]));

                    String regex = params[5];
                    helper.write_to_hdfs("job.xml", regex);

                    jobtracker_stub.jobSubmit(job_request.build().toByteArray());
                    break;
                case "exit":
                /* We are done Exit the client */
                    break label;
                default:
                    err.println("Invalid Command");
                    err.println("Commands Allowed are :");
                    err.println("put <fileName>");
                    err.println("get <fileName>");
                    err.println("list");
                    err.println("exit");
                    break;
            }
        }
    }
}
