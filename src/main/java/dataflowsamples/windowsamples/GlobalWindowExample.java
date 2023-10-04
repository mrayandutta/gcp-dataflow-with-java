package dataflowsamples.windowsamples;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.time.LocalDate;

public class GlobalWindowExample {
    public static void main(String[] args) {
        // Create a pipeline
        Pipeline pipeline = Pipeline.create();

        // Sample input data
        PCollection<String> inputData = pipeline.apply(Create.of(
                "17:00:00,User1,PageView",
                "17:00:20,User2,PageView",
                "17:01:20,User1,ButtonClick",
                "17:03:00,User1,ButtonClick",
                "17:10:00,User2,ButtonClick",
                "17:12:00,User1,PageView",
                "17:14:00,User1,PageView",
                "17:15:00,User1,PageView"
        ));

        // Parse input data to TimestampedValue
        PCollection<KV<String, String>> userActions = inputData.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] parts = c.element().split(",");
                String time = parts[0];
                String currentDate = LocalDate.now().toString(); // Get current date
                Instant timestamp = Instant.parse(currentDate + "T" + time + ".000Z");
                c.outputWithTimestamp(KV.of(parts[1], parts[2]), timestamp);
            }
        }));

        // Group actions by user in a global window
        PCollection<KV<String, Iterable<String>>> globalActions = userActions.apply(GroupByKey.create());

        // Print actions by user in global window
        globalActions.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String user = c.element().getKey();
                Iterable<String> actions = c.element().getValue();

                System.out.println("Actions for " + user + " in Global Window:");
                for (String action : actions) {
                    System.out.println("\t- " + action);
                }
            }
        }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}

