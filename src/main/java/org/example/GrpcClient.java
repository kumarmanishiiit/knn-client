package org.example;

import com.assignment.knn.model.KNNResponse;
import com.assignment.knn.model.KNNServiceGrpc;
import com.assignment.knn.model.Output;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

public class GrpcClient {

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

        com.assignment.knn.model.DataPoint request = com.assignment.knn.model.DataPoint.newBuilder().setXCord(x_cord).setYCord(y_cord).build();

        knnServiceStub.populateData(request, new StreamObserver<Output>() {
            @Override
            public void onNext(Output output) {
                System.out.println(output.getValue());

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });

        Thread.sleep(1000);
    }

    public void knnQuery(CountDownLatch latch, Float x_cord, Float y_cord) throws InterruptedException {

        com.assignment.knn.model.DataPoint dataPoint = com.assignment.knn.model.DataPoint.newBuilder().setXCord(x_cord).setYCord(y_cord).build();

        com.assignment.knn.model.KNNRequest request = com.assignment.knn.model.KNNRequest.newBuilder().setDataPoint(dataPoint).setK(2).build();


        knnServiceStub.findKNearestNeighbors(request, new StreamObserver<KNNResponse>() {

            @Override
            public void onNext(KNNResponse knnResponse) {
                knnResponses = knnResponse;
                System.out.println(knnResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        Thread.sleep(1000);
    }

    public void shutdown() {
        channel.shutdown();
    }

//    public void makeNonBlockingCalls(Float x_cord, Float y_cord) throws InterruptedException {
//        // Create a CountDownLatch with count 2 since we have two non-blocking calls
//        CountDownLatch latch = new CountDownLatch(2);
//
//        knnQuery(latch, x_cord, y_cord);
//
//        // Wait for both calls to complete (latch to reach 0)
//        latch.await();
//
//        // Process the responses once both are received
////        processAggregatedResponses(responseFromServer1, responseFromServer2);
//    }

}
