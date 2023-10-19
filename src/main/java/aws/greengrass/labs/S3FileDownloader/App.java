package aws.greengrass.labs.S3FileDownloader;

import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.SubscribeToIoTCoreResponseHandler;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToIoTCoreRequest;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToIoTCoreResponse;
import software.amazon.awssdk.aws.greengrass.model.QOS;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.util.List;
import java.util.ArrayList;

public class App {
    public static void main(String[] args) {
        // Set the topic and destination folder constants with the arguments passed by recipe file
        String downloadTopic = args[0];
        String pauseTopic = args[1];
        String jobUpdateTopic = args[2];
        String destinationFolder = args[3];

        // Subscribe to the local IPC topics
        String[] topics = {downloadTopic, pauseTopic};
        List<SubscribeToIoTCoreResponseHandler> responseHandlers = 
                            new ArrayList<SubscribeToIoTCoreResponseHandler>();
        
        try {
            GreengrassCoreIPCClientV2 ipcClient = GreengrassCoreIPCClientV2.builder().build();
            IoTManager iotManager = new IoTManager(ipcClient, downloadTopic, pauseTopic, jobUpdateTopic, destinationFolder);
            for (String topic : topics) {
                System.out.println("Subscribing to topic: " + topic);
                SubscribeToIoTCoreRequest request = new SubscribeToIoTCoreRequest();
                request.setTopicName(topic);
                request.setQos(QOS.AT_LEAST_ONCE);
                GreengrassCoreIPCClientV2.StreamingResponse<SubscribeToIoTCoreResponse,
                        SubscribeToIoTCoreResponseHandler> response =
                        ipcClient.subscribeToIoTCore(request, iotManager);
                SubscribeToIoTCoreResponseHandler responseHandler = response.getHandler();
                responseHandlers.add(responseHandler);
                System.out.println("Successfully subscribed to topic: " + topic);
            }
            
            // Keep the main thread alive, or the process will exit.
            try {
                while (true) {
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                System.out.println("Subscribe interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                // To stop subscribing, close the stream.
                for(SubscribeToIoTCoreResponseHandler responseHandler : responseHandlers){
                    responseHandler.closeStream();
                }
            }
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (e.getCause() instanceof UnauthorizedError) {
                System.err.println("Unauthorized error while subscribing iot topic.");
            } else {
                System.err.println(e.getMessage());
                System.err.println("Exception occurred when using IPC.");
            }
            System.exit(1);
        } 
    }
}
