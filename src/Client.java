import HDFS.hdfs;

import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

/**
 * Created by harry7 on 9/10/16.
 */


public class Client {
    private static String namenode_ip;
    private static int block_size = 16777216; /* 16 KB */

    public static void main(String[] args) throws RemoteException, NotBoundException {
        Registry reg = LocateRegistry.getRegistry(namenode_ip);
        Namenodedef namenode_stub = (Namenodedef) reg.lookup("NameNode");
        Scanner in = new Scanner(System.in);
        String command, file_name;
        PrintStream err = new PrintStream(System.err);
        for (; ; ) {
            command = in.next();
            if (command.equals("put")) {
                file_name = in.next();
                /* Write the stuff. */
            } else if (command.equals("get")) {
                file_name = in.next();
                /* Get The Stuff */

            } else if (command.equals("list")) {
                /* List the Stuff */
                try {
                    byte[] response = namenode_stub.list(null);
                    hdfs.ListFilesResponse listfile_response = hdfs.ListFilesResponse.parseFrom(response);
                    for (String filename : listfile_response.getFileNamesList()) {
                        err.println(filename);
                    }
                    err.println("list done");
                } catch (Exception e) {

                }
            } else if (command.equals("exit")) {
                /* We are done Exit the client */
                break;
            } else {
                err.println("Invalid Command");
                err.println("Commands Allowed are :");
                err.println("put <fileName>");
                err.println("get <fileName>");
                err.println("list");
                err.println("exit");
            }
        }
    }
}
