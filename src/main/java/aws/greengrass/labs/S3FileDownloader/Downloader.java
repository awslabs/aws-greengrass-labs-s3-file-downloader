package aws.greengrass.labs.S3FileDownloader;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.ResumableFileDownload;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferProgress;

import java.nio.file.Paths;
import java.util.OptionalDouble;

import java.io.File;

public class Downloader extends Thread{
    String bucketName;
    String keyName;
    String jobId;
    String filePath;
    String jobUpdateTopic;
    IoTManager iotManager;

    public Downloader(String jobUpdate, String bucketName, String keyName, String jobId, String filePath, IoTManager iotManager){
        super(jobId);
        this.jobId = jobId;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.filePath = filePath;
        this.jobUpdateTopic = jobUpdate;
        this.iotManager = iotManager;
    }

    // Thread starting point
    public void run(){
        // Initiate the download job
        downloadFile(bucketName, keyName, jobId, filePath);
    }

    // This method starts downloading a file from an S3 bucket based on the job document.
    // It uses S3 Transfer Manager to manage downloads. The download runs in its own thread.
    // The download can be interrupted by the StreamHandler.java onStreamEvent method when there
    // is a paused state in the shadow document.
    private void downloadFile(String bucket_name, String key_name, String job_id,
                                    String file_path) {
        System.out.println("Downloading " + bucket_name + " " + key_name);

        new File(file_path).getParentFile().mkdirs();

        S3TransferManager transferManager = S3TransferManager.create();
        FileDownload xfer = null;
        try {
            if (isResume(file_path)){ // Resume if it is a resume job
                System.out.println("Resuming... " + bucket_name + " " + key_name);
                ResumableFileDownload resumableFileDownload = ResumableFileDownload.fromFile(Paths.get(file_path+".resume-download"));
                xfer = transferManager.resumeDownloadFile(resumableFileDownload);
            } else { // It is a new job, start downloading
                software.amazon.awssdk.transfer.s3.model.DownloadFileRequest.Builder builder = 
                                            DownloadFileRequest.builder()
                                            .getObjectRequest(req -> req.bucket(bucket_name).key
                                            (key_name))
                                            .destination(Paths.get(file_path));
                DownloadFileRequest downloadFileRequest = builder.build();
                xfer = transferManager.downloadFile(downloadFileRequest);
            }
            do {
                Thread.sleep(1000);
                TransferProgress progress = xfer.progress();
                OptionalDouble pct = progress.snapshot().ratioTransferred();
                System.out.println("Progress "+ pct);
                iotManager.publishUpdate(job_id, "IN_PROGRESS", Double.toString(pct.orElse(0.0)));
            } while (xfer.completionFuture().isDone() == false);
            if (xfer.completionFuture().isCompletedExceptionally()){
                iotManager.publishUpdate(job_id, "FAILED", "fail");
            } else if (xfer.completionFuture().isDone()){
                iotManager.publishUpdate(job_id, "SUCCEEDED", "done");
            }
        } catch (InterruptedException e) {
            // This is called when the thread is interrupted
            // We use it to pause a download
            pauseAndPersistDownload(xfer, file_path);
            iotManager.publishUpdate(job_id, "IN_PROGRESS", "paused");
            Thread.currentThread().interrupt();
        } catch (java.lang.IllegalMonitorStateException e){
            System.out.println("Exception caught : " + e.getMessage());
        } finally {
            //xfer_mgr.shutdownNow();
            transferManager.close();
            // Notify the application when a download finished
            iotManager.downloadEnd(job_id);
        }
        
    }

    // We detect if the incoming job is a resume job by checking the disk for an existing <MEDIAFILE>.resume-download file
    boolean isResume(String file_path){
        File fp = new File(file_path + ".resume-download");
        return fp.exists();
    }

    // This method pauses the download and writes the persistent file to the disk
    // It can be called when we detect a disconnect during a download or with the pause command
    void pauseAndPersistDownload(FileDownload xfer, String file_path){
        System.out.println("Pausing download...");
        // The download is written to a persistent file
        ResumableFileDownload resumableFileDownload = xfer.pause();
        resumableFileDownload.serializeToFile(Paths.get(file_path + ".resume-download"));
        System.out.println("Resume file written!");
        
    }

    

}
