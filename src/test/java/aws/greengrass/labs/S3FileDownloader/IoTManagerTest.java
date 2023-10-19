package aws.greengrass.labs.S3FileDownloader;

import org.junit.*;
import static org.junit.Assert.*;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.gson.Gson;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.IoTCoreMessage;
import software.amazon.awssdk.aws.greengrass.model.MQTTMessage;
import software.amazon.awssdk.aws.greengrass.model.PublishToIoTCoreRequest;
import software.amazon.awssdk.aws.greengrass.model.QOS;

public class IoTManagerTest {
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    
    GreengrassCoreIPCClientV2 ipcClientMock;
    IoTManager iotManager;

    // Test constants
    String downloadTopic = "things/TestThing/download";
    String pauseTopic = "things/TestThing/pause";
    String updateTopic = "things/TestThing/update";
    String bucketName = "test-bucket";
    String key = "file.zip";
    String jobId = "download1";
    String jobId2 = "download2";
    String jobId3 = "download3";
    String path = "/home/ggc_user/destination/";
    String status = "SUCCEEDED";
    String reason = "testreason";


    @Before
    public void setup() {
        System.setOut(new PrintStream(outputStreamCaptor));

        // mock the ipcClient
        ipcClientMock = Mockito.mock(GreengrassCoreIPCClientV2.class);
        iotManager = new IoTManager(ipcClientMock, downloadTopic, pauseTopic, updateTopic, path);
    }
    
    @Test
    public void testOnStreamEventDownload() {

        // Call onStreamEvent with the download message
        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId + "\", \"s3Bucket\":\"" + bucketName + "\", \"key\":\"" + key + "\"}", 
                downloadTopic));

        // Check if we could start a Downloader thread and add it to the list
        assertFalse(iotManager.downloaderThreadList.isEmpty());

        // Get the Downloader thread
        Downloader downloader = (Downloader) iotManager.downloaderThreadList.get(0);

        // Test variables that were set by the IoT Core Message
        assertTrue(downloader.bucketName.equals(bucketName));
        assertTrue(downloader.jobId.equals(jobId));
        assertTrue(downloader.keyName.equals(key));
        assertTrue(downloader.filePath.contains(path) && downloader.filePath.contains(key));

        // Call downloadEnd to finish the download
        downloader.iotManager.downloadEnd(jobId);

        downloader.interrupt();

        // Check if the thread list is empty after calling downloadEnd
        assertTrue(iotManager.downloaderThreadList.isEmpty());
    } 

    @Test
    public void testOnStreamEventPause() {

        // Call onStreamEvent with the download message
        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId + "\", \"s3Bucket\":\"" + bucketName + "\", \"key\":\"" + key + "\"}", 
                downloadTopic));

        // Check if we could start a Downloader thread and add it to the list
        assertFalse(iotManager.downloaderThreadList.isEmpty());

        // Get the Downloader thread
        Downloader downloader = (Downloader) iotManager.downloaderThreadList.get(0);

        // Test variables that were set by the IoT Core Message
        assertTrue(downloader.bucketName.equals(bucketName));
        assertTrue(downloader.jobId.equals(jobId));
        assertTrue(downloader.keyName.equals(key));
        assertTrue(downloader.filePath.contains(path) && downloader.filePath.contains(key));

        iotManager.onStreamEvent(
            generateTestMessage(
                "{}", 
                pauseTopic));

        // Check if the thread list is empty after calling downloadEnd
        assertTrue(iotManager.downloaderThreadList.isEmpty());
    } 

    @Test
    public void testOnStreamEventParallelDownload() {
        
        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId + "\", \"s3Bucket\":\"" + bucketName + "\", \"key\":\"" + key + "\"}", 
                downloadTopic));

        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId2 + "\", \"s3Bucket\":\"" + bucketName + "\", \"key\":\"" + key + "\"}", 
                downloadTopic));

        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId3 + "\", \"s3Bucket\":\"" + bucketName + "\", \"key\":\"" + key + "\"}", 
                downloadTopic));


        // Check if we could start a Downloader thread and add it to the list
        assertTrue(iotManager.downloaderThreadList.size() == 3);

        // Get the Downloader thread
        Downloader downloader = (Downloader) iotManager.downloaderThreadList.get(0);

        // Test variables that were set by the IoT Core Message
        assertTrue(downloader.bucketName.equals(bucketName));
        assertTrue(downloader.jobId.equals(jobId));
        assertTrue(downloader.keyName.equals(key));
        assertTrue(downloader.filePath.contains(path) && downloader.filePath.contains(key));

        iotManager.onStreamEvent(
            generateTestMessage(
                "{}", 
                pauseTopic));

        // Check if the thread list is empty after calling downloadEnd
        assertTrue(iotManager.downloaderThreadList.isEmpty());
    } 

    @Test
    public void testOnStreamEventCorruptedPayload() {
        System.setErr(new PrintStream(outputStreamCaptor));

        // Test onStreamEvent with a corrupted payload
        iotManager.onStreamEvent(
            generateTestMessage(
                "{\"jobId\":\"" + jobId + "\", \"s3Bucket\":\"", 
                downloadTopic));

        // Check if exception occured and handled due to corrupted payload
        assertTrue(outputStreamCaptor.toString().trim().contains("Exception occurred while processing subscription response message."));
                
        // The payload was corrupted, we don't expect any downloader task to start
        assertTrue(iotManager.downloaderThreadList.isEmpty());
    } 

    @Test
    public void testPublishUpdateToIoTCore() throws InterruptedException {
        System.setErr(new PrintStream(outputStreamCaptor));

        JobUpdate update = new JobUpdate(jobId, status, reason);
        Gson gson = new Gson();
        String message = gson.toJson(update);

        PublishToIoTCoreRequest publishToTopicRequest = new PublishToIoTCoreRequest();
        publishToTopicRequest.setTopicName(updateTopic);
        publishToTopicRequest.setPayload(message.getBytes(StandardCharsets.UTF_8));
        publishToTopicRequest.setQos(QOS.AT_LEAST_ONCE);

        IoTManager iotManagerMock = Mockito.spy(iotManager);

        iotManagerMock.publishUpdate(jobId, status, reason);

        ArgumentCaptor<String> argTopic = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> argMessage = ArgumentCaptor.forClass(String.class);
        Mockito.verify(iotManagerMock).publishMessageToTopic(argTopic.capture(), argMessage.capture());
        assertEquals(updateTopic, argTopic.getValue());
        assertEquals(message, argMessage.getValue());
    } 

    @Test
    public void testPublishMessageToTopic() throws InterruptedException {
        System.setErr(new PrintStream(outputStreamCaptor));

        JobUpdate update = new JobUpdate(jobId, status, reason);
        Gson gson = new Gson();
        String message = gson.toJson(update);

        iotManager.publishMessageToTopic(updateTopic, message);

        ArgumentCaptor<PublishToIoTCoreRequest> argReq = ArgumentCaptor.forClass(PublishToIoTCoreRequest.class);
        Mockito.verify(ipcClientMock).publishToIoTCore(argReq.capture());
        assertEquals(updateTopic, argReq.getValue().getTopicName());
        assertEquals(message, new String(argReq.getValue().getPayload(), StandardCharsets.UTF_8));
        assertEquals(QOS.AT_LEAST_ONCE, argReq.getValue().getQos());
    } 

    IoTCoreMessage generateTestMessage(String payload, String topic){
        // Create an IoT Core Message
        MQTTMessage mqttMessage = new MQTTMessage().withTopicName(topic).withPayload(payload.getBytes());
        IoTCoreMessage iotCoreMessage = new IoTCoreMessage();
        iotCoreMessage.setMessage(mqttMessage);
        return iotCoreMessage;
    }
}