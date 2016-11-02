import java.rmi.Remote;

public interface Jobtrackerdef extends Remote {

    /* JobSubmitResponse jobSubmit(JobSubmitRequest) */
    byte[] jobSubmit(byte[] inp);

    /* JobStatusResponse getJobStatus(JobStatusRequest) */
    byte[] getJobStatus(byte[] inp);

    /* HeartBeatResponse heartBeat(HeartBeatRequest) */
    byte[] heartBeat(byte[] inp);

}
