import HDFS.hdfs;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by harry7 on 9/10/16.
 */
public class Datanode implements Datanodedef {
    private static String namenode_ip;
    private int my_id;

    public Datanode(int id) {
        my_id = id;
    }

    public byte[] readBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] writeBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public static void main(String[] args) {
        PrintStream err = new PrintStream(System.err);
        if (args.length < 1) {
            err.println("Need Data Node ID as an argument");
            System.exit(-1);
        }
        File file_dir = new File("Blocks");
        if (!file_dir.exists()) {
            boolean status = file_dir.mkdirs();
        }
        int myid = Integer.valueOf(args[0]);
        Datanode obj = new Datanode(myid);
        HeartbeatHandler handler1 = new HeartbeatHandler(myid);
        handler1.start();
        BlockreportHandler handler2 = new BlockreportHandler(myid);
        handler2.start();
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
                    hdfs.HeartBeatRequest.Builder request = hdfs.HeartBeatRequest.newBuilder();
                    request.setId(id);
                    Registry reg = LocateRegistry.getRegistry(namenode_ip);
                    Namenodedef stub = (Namenodedef) reg.lookup("NameNode");
                    stub.heartBeat(request.build().toByteArray());
                    System.err.println("Sent HeartBeat from Node " + id);
                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                }
            } catch (RemoteException | NotBoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class BlockreportHandler extends Thread {
        /* Handles the heart beat */
        private int id;

        BlockreportHandler(int my_id) {
            id = my_id;
        }

        public void run() {
            while (true) {
                /* Send Periodically HeartBeat */
                File blkReport = new File("block_report.txt");
                String data = "";
                if (blkReport.exists() && blkReport.length() != 0) {
                    try {
                        hdfs.BlockReportRequest.Builder request = hdfs.BlockReportRequest.newBuilder();
                        request.setId(id);
                        Registry reg = LocateRegistry.getRegistry(namenode_ip);
                        Namenodedef stub = (Namenodedef) reg.lookup("NameNode");
                        BufferedReader br = new BufferedReader(new FileReader(blkReport));
                        String blockNumber;
                        while ((blockNumber = br.readLine()) != null) {
                            request.addBlockNumbers(Integer.parseInt(blockNumber));
                        }
                        br.close();
                        stub.blockReport(request.build().toByteArray());
                        System.err.println("Sent Block report from Node " + id);
                    } catch (IOException | NotBoundException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("No block report");
                }
                try {
                    Thread.sleep(10000); /* Sleep for 10 Seconds */
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}