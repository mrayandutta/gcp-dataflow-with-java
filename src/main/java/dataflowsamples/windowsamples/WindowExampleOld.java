package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class WindowExampleOld {

    public static void main(String[] args) {

        // Create a pipeline
        Pipeline pipeline = Pipeline.create();
        int FIXED_WINDOW_SIZE = 10*3;

        // Create a PCollection of input data
        PCollection<String> inputData = pipeline
                .apply(Create.of(
                        "17:00:00,Product1,10",
                        "17:00:11,Product1,20",
                        "17:00:11,Product2,10",
                        "17:00:21,Product2,10",
                        "17:00:21,Product1,40",
                        "17:00:30,Product1,10",
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
                LocalDate date = LocalDate.now();  // Get the current date
                LocalDateTime dateTime = LocalDateTime.of(date, time);
                long epochTime = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

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
        PCollection<KV<String, String>> sumOfProductSales = groupedProductSales.apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, String>>() {

            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
                String productName = c.element().getKey();
                Iterable<Integer> productPrices = c.element().getValue();

                int totalSales = 0;
                for (int price : productPrices) {
                    totalSales += price;
                }

                IntervalWindow intervalWindow = (IntervalWindow) window; // Cast the window to IntervalWindow
                String windowInfo = ",Window information [Start: " + intervalWindow.start() + " End: " + intervalWindow.end()+"]";

                c.output(KV.of(productName, totalSales + " " + windowInfo));
            }
        }));


        // Print the sum of the product sales.
        sumOfProductSales.apply(ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Product: " + c.element().getKey() + ", Total Sale: " + c.element().getValue());
            }
        }));


        // Run the pipeline
        pipeline.run();
    }
}
