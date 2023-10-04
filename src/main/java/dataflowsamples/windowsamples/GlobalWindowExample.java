package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class GlobalWindowExample {
    public static void main(String[] args) {
        // Create a pipeline
        Pipeline pipeline = Pipeline.create();

        // Sample shopping app data with date included: "date,time,user,action"
        PCollection<String> inputData = pipeline.apply(Create.of(
                "2023-10-04,09:00:00,User1,ViewLaptop",
                "2023-10-04,09:03:00,User1,AddToCartLaptop",
                "2023-10-04,09:10:00,User1,Checkout",
                "2023-10-04,14:30:00,User1,ViewMobilePhone"
        ));

        // Parse input data to TimestampedValue
        PCollection<KV<String, String>> userActions = inputData.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] parts = c.element().split(",");
                String date = parts[0];
                String time = parts[1];
                Instant timestamp = Instant.parse(date + "T" + time + ".000Z");
                c.outputWithTimestamp(KV.of(parts[2], parts[3]), timestamp);
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
