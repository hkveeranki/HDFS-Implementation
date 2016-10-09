import HDFS.hdfs;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static java.lang.Integer.max;

/**
 * Created by harry7 on 9/10/16.
 */
public class Namenode implements Namenodedef {

    private HashMap<Integer, String> map_handle_filename;
    private static HashMap<String, ArrayList<Integer>> map_filename_blocks;
    private static HashMap<Integer, ArrayList<Integer>> map_block_datanode;
    private static int block_number;
    private int file_number;
    private static String[] datanode_ip = {"127.0.0.1", "127.0.0.1"};
    private static int datanode_num = 3;

    public Namenode() {
        file_number = 0;
        map_handle_filename = new HashMap<>();
    }

    public byte[] openFile(byte[] inp) throws RemoteException {

        return new byte[0];
    }

    public byte[] closeFile(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] getBlockLocations(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] assignBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] list(byte[] inp) throws RemoteException {
        try {
            hdfs.ListFilesResponse.Builder response = hdfs.ListFilesResponse.newBuilder().setStatus(1);
            for (String file : map_filename_blocks.keySet()) {
                response.addFileNames(file);
            }
            return response.build().toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] blockReport(byte[] inp) throws RemoteException {
        try {
            /* Need to Improve this as well */
            hdfs.BlockReportRequest request = hdfs.BlockReportRequest.parseFrom(inp);
            int datanode_id = request.getId();
            for (int blocknum : request.getBlockNumbersList()) {
                if (map_block_datanode.get(blocknum) == null) {
                    map_block_datanode.put(blocknum, new ArrayList<Integer>(Arrays.asList(datanode_id)));
                } else {
                    if (!map_block_datanode.get(blocknum).contains(datanode_id)) {
                        map_block_datanode.get(blocknum).add(datanode_id);
                    }
                }
            }
            System.err.println("Got Block Report from " + datanode_id);
            /* Need to Some thing here */
            hdfs.BlockReportResponse.Builder response = hdfs.BlockReportResponse.newBuilder().addStatus(1);
            return response.build().toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] heartBeat(byte[] inp) throws RemoteException {
        /* Need to acknowledge for heartbeat sent */
        try {
            hdfs.HeartBeatRequest request = hdfs.HeartBeatRequest.parseFrom(inp);
            int datanode_id = request.getId();
            System.err.println("Got Heart Beat from " + datanode_id); /* Need to do something as well */
            hdfs.HeartBeatResponse.Builder response = hdfs.HeartBeatResponse.newBuilder().setStatus(1);
            return response.build().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        File file_list = new File("file_list.txt"); /* To persist the data */
        map_filename_blocks = new HashMap<>();
        block_number = 0;
        map_block_datanode = new HashMap<>();
        /* Write the existing data */
        try {
            boolean status = file_list.createNewFile();
            BufferedReader reader = new BufferedReader(new FileReader(file_list));
            String line, file_name;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(" ");
                file_name = data[0];
                ArrayList<Integer> blocks_data = new ArrayList<>();
                for (int i = 1; i < data.length; i++) {
                    int cur = Integer.valueOf(data[i]);
                    blocks_data.add(cur);
                    block_number = max(cur, block_number); /* Get the Block Number Used so Far */
                }
                map_filename_blocks.put(file_name, blocks_data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Namenode obj = new Namenode();
            Namenodedef stub = (Namenodedef) UnicastRemoteObject.exportObject(obj, 0);
            Registry reg = LocateRegistry.getRegistry();
            reg.rebind("NameNode", stub);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}