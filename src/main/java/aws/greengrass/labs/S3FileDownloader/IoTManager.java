package aws.greengrass.labs.S3FileDownloader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.IoTCoreMessage;
import software.amazon.awssdk.aws.greengrass.model.MQTTMessage;
import software.amazon.awssdk.aws.greengrass.model.PublishToIoTCoreRequest;
import software.amazon.awssdk.aws.greengrass.model.PublishToIoTCoreResponse;
import software.amazon.awssdk.aws.greengrass.model.QOS;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.eventstreamrpc.StreamResponseHandler;

public class IoTManager implements StreamResponseHandler<IoTCoreMessage>{
    GreengrassCoreIPCClientV2 ipcClient; 
    String downloadTopic;
    String pauseTopic;
    String jobUpdateTopic;
    String destinationFolder;
    List<Thread> downloaderThreadList = new ArrayList<Thread>();

    public IoTManager(GreengrassCoreIPCClientV2 ipcClient, 
                String downloadTopic, String pauseTopic, 
                String jobUpdateTopic, String destinationFolder){
        this.ipcClient = ipcClient;
        this.downloadTopic = downloadTopic;
        this.pauseTopic = pauseTopic;
        this.jobUpdateTopic = jobUpdateTopic;
        this.destinationFolder = destinationFolder;
    }

    @Override
    public void onStreamEvent(IoTCoreMessage iotCoremessage) {
        try {
            MQTTMessage mqttMessage = iotCoremessage.getMessage();
            String topic = mqttMessage.getTopicName();
            String message = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
            System.out.printf("Received new message on topic %s: %s%n", topic, message);
            Gson gson = new Gson();
            if (topic.equals(downloadTopic)){ // It is a download job
                System.out.printf("Download job %s: %s%n", topic, message);
                DownloadJob job = gson.fromJson(message, DownloadJob.class);
                for (Thread t : downloaderThreadList) {
                    if (t.getName().equals(job.jobId)){
                        System.out.println("This job is already being processed. jobId: " + job.jobId);
                        return;
                    }
                }
                // We create a download thread for this job
                Downloader downloader = new Downloader(jobUpdateTopic, job.s3Bucket, 
                                        job.key, job.jobId, destinationFolder + job.s3Bucket + "/" + job.key, this);
                downloaderThreadList.add(downloader);
                downloader.start();
            } else if (topic.equals(pauseTopic)){ // It is a pause command
                // We go through all the Downloader threads and send a thread interrupt
                // This will kill all the threads. However, the Downloader class catches 
                // the interrupted exception and writes the persistent download files to 
                // the disk to continue later.
                for (Thread t : downloaderThreadList) {
                    t.interrupt();
                }
                downloaderThreadList.clear();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Exception occurred while processing subscription response message.");
        }
    }

    @Override
    public boolean onStreamError(Throwable error) {
        System.err.println("Received a stream error.");
        return false; // Return true to close stream, false to keep stream open.
    }

    @Override
    public void onStreamClosed() {
        System.out.println("Subscribe to topic stream closed.");
    }

    public void downloadEnd(String jobId) {
        System.out.printf("Download task ended %s %n", jobId);
        // Remove the finished download thread from the list
        downloaderThreadList.removeIf(t -> (t.getName().equals(jobId)));
    }

    // Publishes job status update to the job update topic
    public void publishUpdate(String job_id, String status, String reason){
        String topic = jobUpdateTopic; // See JOB_FEEDBACK_TOPIC parameter in the receipe file
        JobUpdate update = new JobUpdate(job_id, status, reason);
        Gson gson = new Gson();
        String message = gson.toJson(update);
        try{
            publishMessageToTopic(topic, message);
            System.out.println("Successfully published to topic: " + topic + " - " + message);
        } catch (InterruptedException e){
            System.err.println("Interrupted during IPC publish! " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (UnauthorizedError e) {
            System.err.println("Unauthorized error while publishing to topic: " + topic);
        } catch (Exception e){
            System.err.println("Exception occurred when using IPC.");
        }
    }

    // Publishes a payload to a local IPC topic
    public PublishToIoTCoreResponse publishMessageToTopic(String topic, String message) throws InterruptedException {
        PublishToIoTCoreRequest publishToTopicRequest = new PublishToIoTCoreRequest();
        publishToTopicRequest.setTopicName(topic);
        publishToTopicRequest.setPayload(message.getBytes(StandardCharsets.UTF_8));
        publishToTopicRequest.setQos(QOS.AT_LEAST_ONCE);
        return ipcClient.publishToIoTCore(publishToTopicRequest);
    }
    
}
