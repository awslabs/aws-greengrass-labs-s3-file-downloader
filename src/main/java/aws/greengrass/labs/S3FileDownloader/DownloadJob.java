package aws.greengrass.labs.S3FileDownloader;

public class DownloadJob {
    public String jobId;
    public String s3Bucket;
    public String key;
    
    public DownloadJob(String jobId, String s3Bucket, String key){
        this.jobId = jobId;
        this.s3Bucket = s3Bucket;
        this.key = key;
    }
}
