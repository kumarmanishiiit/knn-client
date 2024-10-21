package org.example;

import com.assignment.knn.model.DataPoint;
import com.assignment.knn.model.KNNResponse;
import com.assignment.knn.model.KNNServiceGrpc;
import com.assignment.knn.model.Output;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class GrpcClient {

    private static final Logger log = LoggerFactory.getLogger(GrpcClient.class);

    public KNNResponse knnResponses;

    private final ManagedChannel channel;
    private final KNNServiceGrpc.KNNServiceStub knnServiceStub;

    public GrpcClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.knnServiceStub = KNNServiceGrpc.newStub(channel);
    }

    public void sendData(Float x_cord, Float y_cord) throws InterruptedException {

        DataPoint request = DataPoint.newBuilder().setXCord(x_cord).setYCord(y_cord).build();

        knnServiceStub.populateData(request, new StreamObserver<Output>() {
            @Override
            public void onNext(Output output) {
                log.info("Output to client: {}", output.getValue());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error: {}", throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("Data population has been completed!!");
            }
        });

        Thread.sleep(1000);
    }

    public void knnQuery(CountDownLatch latch, Float x_cord, Float y_cord, int k) throws InterruptedException {

        com.assignment.knn.model.DataPoint dataPoint = com.assignment.knn.model.DataPoint.newBuilder().setXCord(x_cord).setYCord(y_cord).build();

        com.assignment.knn.model.KNNRequest request = com.assignment.knn.model.KNNRequest.newBuilder().setDataPoint(dataPoint).setK(k).build();

        knnServiceStub.findKNearestNeighbors(request, new StreamObserver<KNNResponse>() {

            @Override
            public void onNext(KNNResponse knnResponse) {
                knnResponses = knnResponse;
                log.info("Responding with: {}", knnResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
                log.error("Error: {}", throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                latch.countDown();
                log.info("KNN computation has been completed!!!");
            }
        });

        Thread.sleep(1000);
    }

    public void shutdown() {
        channel.shutdown();
    }

}
