package org.example;

import com.assignment.knn.model.DataPointResponse;
import com.assignment.knn.model.KNNResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KnnClient {

    private static final Logger log = LoggerFactory.getLogger(KnnClient.class);


    static GrpcClient client1;

    static GrpcClient client2;

    static int k;

    static float query1;

    static float query2;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            log.warn("Usage: java -jar MyServer.jar <port> <port>");
            System.exit(1);
        }

        // Read the port number from the command line arguments
        int port1 = 0;
        int port2 = 0;
        try {
            port1 = Integer.parseInt(args[0]);  // Parse the first argument as an integer
            port2 = Integer.parseInt(args[1]);
            k = Integer.parseInt(args[2]);
            query1 = Float.parseFloat(args[3]);
            query2 = Float.parseFloat(args[4]);

        } catch (NumberFormatException e) {
            log.warn("Error: Port must be a number.");
            System.exit(1);
        }

        client1 = new GrpcClient("localhost", port1); // Server 1
        client2 = new GrpcClient("localhost", port2); // Server 2

        sendDataToServers();
    }

    // Method to send data to the gRPC servers
    public static void sendDataToServers() throws InterruptedException {

        long startTime = System.currentTimeMillis();

        // Number of server should be under latch.
        // We are using this for holding the call until all the worker respond.
        CountDownLatch latch = new CountDownLatch(2);

        client1.knnQuery(latch, query1, query2, k);
        client2.knnQuery(latch, query1, query2, k);

        latch.await();

        com.assignment.knn.model.KNNResponse knnResponse1 = client1.knnResponses;
        com.assignment.knn.model.KNNResponse knnResponse2 = client2.knnResponses;


        if(knnResponse1 == null || knnResponse2 == null) {
            log.warn("KNN Response is null. Please try again!!!");
            System.exit(1);
        }
        List<DataPointResponse> dataPointResponse = new ArrayList<>(knnResponse1.getKDataPointList());
        List<DataPointResponse> reponse1 = knnResponse2.getKDataPointList();
        if(reponse1 != null) {
            dataPointResponse.addAll(reponse1);
        }
        PriorityQueue<com.assignment.knn.model.DataPointResponse> maxHeap = new PriorityQueue<>(2, new Comparator<com.assignment.knn.model.DataPointResponse>() {
            @Override
            public int compare(com.assignment.knn.model.DataPointResponse o1, com.assignment.knn.model.DataPointResponse o2) {
                return Float.compare(o2.getDistance(), o1.getDistance());
            }
        });

        for (com.assignment.knn.model.DataPointResponse pointResponse : dataPointResponse) {
            maxHeap.add(pointResponse);
            // If heap size exceeds k, remove the smallest element
            if (maxHeap.size() > k) {
                maxHeap.poll();
            }
        }

        long endTime = System.currentTimeMillis();

        log.info("Latency: is: {} ms", (endTime - startTime));

        log.info("{} Nearest neighbour for {}, {} are: ", k, query1, query2);
        for (int i = 0; i < k; i++) {
            if(!maxHeap.isEmpty()) {
                log.info(String.valueOf(Objects.requireNonNull(maxHeap.poll()).getDataPoint()));
            }
        }
    }


}

