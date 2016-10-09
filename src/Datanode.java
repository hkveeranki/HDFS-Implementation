import java.rmi.RemoteException;

/**
 * Created by harry7 on 9/10/16.
 */
public class Datanode implements Datanodedef {
    public byte[] readBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public byte[] writeBlock(byte[] inp) throws RemoteException {
        return new byte[0];
    }
}
