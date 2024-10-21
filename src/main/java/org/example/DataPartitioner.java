package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataPartitioner {

    public static void main(String[] args) throws InterruptedException {
        Map<Float, Float> data = Map.of(0.12f, 34.0f, 42.0f, 1.24f, 5.0f, -78.4f, 21.72f, -18.76f, 61.45f, 74.9f);

        // Now send these datasets to different servers
        sendDataToServers(data);
    }

    // Method to send data to the gRPC servers
    public static void sendDataToServers(Map<Float, Float> data) throws InterruptedException {
        GrpcClient client1 = new GrpcClient("localhost", 6565); // Server 1
        GrpcClient client2 = new GrpcClient("localhost", 7575); // Server 2


        // Convert map entries to a list for splitting
        List<Map.Entry<Float, Float>> entryList = new ArrayList<>(data.entrySet());

        // Split index
        int splitIndex = entryList.size() / 2;

        // Populate the first map with the first half of the entries
        for (int i = 0; i < splitIndex; i++) {
            Map.Entry<Float, Float> entry = entryList.get(i);
            client1.sendData(entry.getKey(), entry.getValue());
        }

        // Populate the second map with the second half of the entries
        for (int i = splitIndex; i < entryList.size(); i++) {
            Map.Entry<Float, Float> entry = entryList.get(i);
            client2.sendData(entry.getKey(), entry.getValue());
        }
    }


}

