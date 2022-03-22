package tempt.examples;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Subscription;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Consumer1 {
    public static void main(String[] args) throws Exception {

        final String serviceUrl = "192.168.0.104:6570,192.168.0.250:6570,192.168.0.223:6570";
        var client = HStreamClient.builder().serviceUrl(serviceUrl).build();
        var streamName = "demo1";
        var subId = UUID.randomUUID().toString();
        var subscription = Subscription.newBuilder().subscription(subId).stream(streamName).build();
        client.createSubscription(subscription);
        var count = 10000000;
        CountDownLatch latch = new CountDownLatch(count);
        var consumer = client.newConsumer()
                .subscription(subId)
                .hRecordReceiver((receivedHRecord, responder) -> {
                            latch.countDown();
                            System.out.println(receivedHRecord.getRecordId());
                        }
                )
                .build();

        System.out.println("consumer starting ...");
        consumer.startAsync().awaitRunning();
        latch.await();
        consumer.stopAsync().awaitTerminated();

        System.out.println("consumer done");

        client.close();
    }
}
