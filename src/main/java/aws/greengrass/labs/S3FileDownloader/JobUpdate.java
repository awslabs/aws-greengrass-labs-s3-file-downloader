package aws.greengrass.labs.S3FileDownloader;

public class JobUpdate {
    public String jobId;
    public String status;
    public String reason;

    public JobUpdate(String jobId, String status, String reason){
        this.jobId = jobId;
        this.status = status;
        this.reason = reason;
    }

}
