package aws.greengrass.labs.S3FileDownloader;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.mockito.Mockito.never;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.ResumableFileDownload;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferProgress;
import software.amazon.awssdk.transfer.s3.progress.TransferProgressSnapshot;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest.Builder;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;

import java.nio.file.Path;
import java.util.OptionalDouble;
import java.util.concurrent.CompletableFuture;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.Thread;

import java.io.File;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Downloader.class, ResumableFileDownload.class, Builder.class, S3TransferManager.class, Thread.class})
@PowerMockIgnore({"jdk.internal.reflect.*"})
public class DownloaderTest {
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    
    GreengrassCoreIPCClientV2 ipcClientMock;
    IoTManager iotManagerMock;
    S3TransferManager transferManagerMock;
    FileDownload xferMock;
    ResumableFileDownload resumableFileDownloadMock;
    TransferProgress xferProgressMock;
    DownloadFileRequest downloadFileRequestMock;
    TransferProgressSnapshot xferProgressSnapshot;
    software.amazon.awssdk.transfer.s3.model.DownloadFileRequest.Builder builderMock;
    CompletableFuture<CompletedFileDownload> completionFutureMock;

    @Before
    public void setup() {
        System.setOut(new PrintStream(outputStreamCaptor));
        
        ipcClientMock = Mockito.mock(GreengrassCoreIPCClientV2.class);
        iotManagerMock = Mockito.mock(IoTManager.class);
        transferManagerMock = Mockito.mock(S3TransferManager.class);
        xferMock = Mockito.mock(FileDownload.class);
        xferProgressMock = Mockito.mock(TransferProgress.class);
        xferProgressSnapshot = Mockito.mock(TransferProgressSnapshot.class);
        builderMock = Mockito.mock(software.amazon.awssdk.transfer.s3.model.DownloadFileRequest.Builder.class);
        completionFutureMock = Mockito.mock(CompletableFuture.class);
        resumableFileDownloadMock = Mockito.mock(ResumableFileDownload.class);

        PowerMockito.mockStatic(Builder.class);
        PowerMockito.mockStatic(S3TransferManager.class);
        PowerMockito.mockStatic(ResumableFileDownload.class);
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.mockStatic(File.class);
    }

    @Test
    public void downloadFileTest() throws InterruptedException {
        Mockito.when(S3TransferManager.create()).thenReturn(transferManagerMock);
        Mockito.when(transferManagerMock.downloadFile(Mockito.any(DownloadFileRequest.class))).thenReturn(xferMock);
        
        Mockito.when(completionFutureMock.isDone()).thenReturn(true);
        Mockito.when(xferMock.completionFuture()).thenReturn(completionFutureMock);

        Mockito.when(xferMock.progress()).thenReturn(xferProgressMock);
        Mockito.when(xferProgressMock.snapshot()).thenReturn(xferProgressSnapshot);
        OptionalDouble od = OptionalDouble.of(0.5);
        Mockito.when(xferProgressSnapshot.ratioTransferred()).thenReturn(od);

        PowerMockito.when(builderMock.build()).thenReturn(downloadFileRequestMock);

        PowerMockito.doNothing().when(Thread.class);
        Thread.sleep(Mockito.anyLong());

        Downloader downloader = new Downloader("jobUpdateTopic", "bucketName", "keyName", "jobId", "filePath", iotManagerMock);

        Downloader downloaderSpy = PowerMockito.spy(downloader);
        
        PowerMockito.doReturn(false).when(downloaderSpy).isResume(Mockito.anyString());

        downloaderSpy.run();
        
        Mockito.verify(transferManagerMock, Mockito.times(1)).downloadFile(Mockito.any(DownloadFileRequest.class));
        Mockito.verify(transferManagerMock, never()).resumeDownloadFile(Mockito.any(ResumableFileDownload.class));
    }

    @Test
    public void resumeFileTest() throws InterruptedException {
        Mockito.when(S3TransferManager.create()).thenReturn(transferManagerMock);
        Mockito.when(transferManagerMock.resumeDownloadFile(Mockito.any(ResumableFileDownload.class))).thenReturn(xferMock);
        
        Mockito.when(completionFutureMock.isDone()).thenReturn(true);
        Mockito.when(xferMock.completionFuture()).thenReturn(completionFutureMock);

        Mockito.when(xferMock.progress()).thenReturn(xferProgressMock);
        Mockito.when(xferProgressMock.snapshot()).thenReturn(xferProgressSnapshot);
        OptionalDouble od = OptionalDouble.of(0.5);
        Mockito.when(xferProgressSnapshot.ratioTransferred()).thenReturn(od);

        PowerMockito.when(builderMock.build()).thenReturn(downloadFileRequestMock);

        PowerMockito.when(ResumableFileDownload.fromFile(Mockito.any(Path.class))).thenReturn(resumableFileDownloadMock);

        PowerMockito.doNothing().when(Thread.class);
        Thread.sleep(Mockito.anyLong());

        Downloader downloader = new Downloader("jobUpdateTopic", "bucketName", "keyName", "jobId", "filePath", iotManagerMock);

        Downloader downloaderSpy = PowerMockito.spy(downloader);
        
        PowerMockito.doReturn(true).when(downloaderSpy).isResume(Mockito.anyString());

        downloaderSpy.run();
        
        Mockito.verify(transferManagerMock, never()).downloadFile(Mockito.any(DownloadFileRequest.class));
        Mockito.verify(transferManagerMock, Mockito.times(1)).resumeDownloadFile(Mockito.any(ResumableFileDownload.class));
    }

    @Test
    public void pauseFileTest() throws InterruptedException {
        Mockito.when(S3TransferManager.create()).thenReturn(transferManagerMock);
        Mockito.when(transferManagerMock.downloadFile(Mockito.any(DownloadFileRequest.class))).thenReturn(xferMock);
        
        Mockito.when(completionFutureMock.isDone()).thenReturn(false);
        Mockito.when(xferMock.completionFuture()).thenReturn(completionFutureMock);

        Mockito.when(xferMock.progress()).thenReturn(xferProgressMock);
        Mockito.when(xferProgressMock.snapshot()).thenReturn(xferProgressSnapshot);
        OptionalDouble od = OptionalDouble.of(0.5);
        Mockito.when(xferProgressSnapshot.ratioTransferred()).thenReturn(od);

        Mockito.when(xferMock.pause()).thenReturn(resumableFileDownloadMock);
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                return null;
            }
        }).when(resumableFileDownloadMock).serializeToBytes();

        PowerMockito.when(builderMock.build()).thenReturn(downloadFileRequestMock);

        PowerMockito.doThrow(new InterruptedException("Testing pause")).when(Thread.class);
        Thread.sleep(Mockito.anyLong());

        Downloader downloader = new Downloader("jobUpdateTopic", "bucketName", "keyName", "jobId", "filePath", iotManagerMock);

        Downloader downloaderSpy = PowerMockito.spy(downloader);

        PowerMockito.doReturn(false).when(downloaderSpy).isResume(Mockito.anyString());

        downloaderSpy.run();
        
        Mockito.verify(downloaderSpy, Mockito.times(1)).pauseAndPersistDownload(Mockito.any(FileDownload.class), Mockito.anyString());
        Mockito.verify(xferMock, Mockito.times(1)).pause();
        Mockito.verify(transferManagerMock, Mockito.times(1)).downloadFile(Mockito.any(DownloadFileRequest.class));
        Mockito.verify(transferManagerMock, never()).resumeDownloadFile(Mockito.any(ResumableFileDownload.class));
    }
}
