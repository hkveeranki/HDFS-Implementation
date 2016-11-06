import HDFS.hdfs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Helper {

    Namenodedef namenode_stub;

    Helper(Namenodedef obj) {
        namenode_stub = obj;
    }

    String read_from_hdfs(String file_name) {
        String read_data = "";
        PrintStream err = new PrintStream(System.err);
        hdfs.OpenFileRequest.Builder openFileRequest = hdfs.OpenFileRequest.newBuilder();
        openFileRequest.setForRead(true);
        openFileRequest.setFileName(file_name);
        try {
            byte[] openFileResponseBytes = namenode_stub.openFile(openFileRequest.build().toByteArray());
            if (openFileResponseBytes != null) {
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
                            read_data += data.toString();
                        } else {
                            err.println("Error Getting read from DataNode: " + dnLocation.getIp());
                        }
                    }
                } else {
                    err.println("Unable to get the Block Locations");
                }
            } else {
                err.println("OpenFile Request failed at NameNode");
            }
        } catch (IOException | NotBoundException e) {
            e.printStackTrace();
        }
        return read_data;
    }

    boolean write_to_hdfs(String file_name, String data) {
        int block_size = 16384;
        try {
            hdfs.OpenFileRequest.Builder request = hdfs.OpenFileRequest.newBuilder();
            request.setForRead(false);
            request.setFileName(file_name);
            byte[] open_resp = namenode_stub.openFile(request.build().toByteArray());
            if (open_resp != null) {
                hdfs.OpenFileResponse response = hdfs.OpenFileResponse.parseFrom(open_resp);
                int handle = response.getHandle();
                hdfs.AssignBlockRequest.Builder assignBlockRequest = hdfs.AssignBlockRequest.newBuilder();
                assignBlockRequest.setHandle(handle);
                byte[] byte_data = data.getBytes();
                int len = byte_data.length;
                int i = 0, bytes_read;
                while (len > 0) {
                    /* Split the data into block size chunks and then write it correspondingly */
                    byte[] read_bytes;
                    if (len >= block_size) {
                        read_bytes = Arrays.copyOfRange(byte_data, i, i + block_size);
                        i += block_size;
                        len -= block_size;
                        bytes_read = block_size;
                    } else {
                        read_bytes = Arrays.copyOfRange(byte_data, i, i + len);
                        i += len;
                        bytes_read = len;
                        len = 0;
                    }
                    byte[] resp = namenode_stub.assignBlock(assignBlockRequest.build().toByteArray());
                    if (resp != null) {
                        hdfs.AssignBlockResponse blockResponse = hdfs.AssignBlockResponse.parseFrom(resp);
                        hdfs.BlockLocations loc = blockResponse.getNewBlock();
                        Registry reg = LocateRegistry.getRegistry(loc.getLocations(0).getIp(), loc.getLocations(0).getPort());
                        Datanodedef datanode_stub = (Datanodedef) reg.lookup("DataNode");
                        hdfs.WriteBlockRequest.Builder writeBlockRequest = hdfs.WriteBlockRequest.newBuilder().setReplicate(true);
                        writeBlockRequest.addData(ByteString.copyFrom(Arrays.copyOfRange(read_bytes, 0, bytes_read)));
                        writeBlockRequest.setBlockInfo(loc);
                        resp = datanode_stub.writeBlock(writeBlockRequest.build().toByteArray());
                        if (resp == null) {
                            System.err.println("Write Block Failed");
                        } else {
                            return true;
                        }
                    } else {
                        System.err.println("Assign block Failed");
                    }
                }
            }
        } catch (RemoteException | InvalidProtocolBufferException | NotBoundException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }
}