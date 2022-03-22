package tempt.examples;

import io.hstream.BatchSetting;
import io.hstream.FlowControlSetting;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Record;
import io.hstream.RecordId;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Producer1 {

    public static void main(String[] args) throws Exception {

        final String serviceUrl = "192.168.0.104:6570,192.168.0.250:6570,192.168.0.223:6570";
        var client = HStreamClient.builder().serviceUrl(serviceUrl).build();
        var streamName = UUID.randomUUID().toString();
        client.createStream(streamName);
        var producer =
                client.newBufferedProducer()
                        .stream(streamName)
                        .batchSetting(BatchSetting.newBuilder().bytesLimit(409600).recordCountLimit(-1).ageLimit(-1).build())
                        .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(81920000).build())
                        .build();
        int count = 10000000;
        ArrayList<CompletableFuture<RecordId>> futures = new ArrayList<>(count);
        System.out.println("begin write...");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            long time = new Date().getTime();
            HRecord hRecord = HRecord.newBuilder()
                    .put("time", time)
                    .put("id", i)
                    .put("test", 10)
                    .build();
            Record record = Record.newBuilder().orderingKey("k-" + i % 20).hRecord(hRecord).build();
            CompletableFuture<RecordId> future = producer.write(record);
            futures.add(future);
        }
        producer.flush();

        for (var future : futures) {
            future.join();
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("wrote done: total %d records, avg rate: %f records/s ", count, count * 1000.0 / (endTime - startTime)));
        producer.close();
        client.close();
    }
}