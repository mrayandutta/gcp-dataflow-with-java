package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.time.LocalTime;

public class WindowExample {

    public static void main(String[] args) {

        // Create a pipeline
        Pipeline pipeline = Pipeline.create();
        int FIXED_WINDOW_SIZE = 10*3;

        // Create a PCollection of input data
        PCollection<String> inputData = pipeline
                .apply(Create.of(
                        "17:00:00,Product1,10",
                        "17:00:11,Product1,20",
                        "17:00:21,Product1,40",
                        "17:00:31,Product1,10",
                        "17:00:41,Product1,10",
                        "17:00:50,Product1,10"
                ));

        // Parse the input data into TimestampedValue KVs
        PCollection<KV<String, Integer>> productSales = inputData.apply(ParDo.of(new DoFn<String, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                String[] parts = line.split(",");
                String[] timeParts = parts[0].split(":");
                LocalTime time = LocalTime.of(
                        Integer.parseInt(timeParts[0]), // hour
                        Integer.parseInt(timeParts[1]), // minute
                        Integer.parseInt(timeParts[2])  // second
                );
                long epochTime = time.toSecondOfDay() * 1000L; // converting to milliseconds
                c.outputWithTimestamp(
                        KV.of(parts[1], Integer.parseInt(parts[2])),
                        Instant.ofEpochMilli(epochTime)
                );
            }
        }));

        PCollection<KV<String, Integer>> windowedProductSales = productSales.apply(
                Window.into(FixedWindows.of(Duration.standardSeconds(FIXED_WINDOW_SIZE)))
        );

        // Group the windowed product sales by product name.
        PCollection<KV<String, Iterable<Integer>>> groupedProductSales = windowedProductSales.apply(GroupByKey.create());

        // Sum the prices of each product in the window.
        PCollection<KV<String, Integer>> sumOfProductSales = groupedProductSales.apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String productName = c.element().getKey();
                Iterable<Integer> productPrices = c.element().getValue();

                int totalSales = 0;
                for (int price : productPrices) {
                    totalSales += price;
                }

                c.output(KV.of(productName, totalSales));
            }
        }));

        // Print the sum of the product sales.
        sumOfProductSales.apply(ParDo.of(new DoFn<KV<String, Integer>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Product: " + c.element().getKey() + ", Total sales: " + c.element().getValue());
            }
        }));

        // Run the pipeline
        pipeline.run();
    }
}
