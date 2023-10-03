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

public class FixedWindowSample {

    public static void main(String[] args) {

        // Create a pipeline
        Pipeline pipeline = Pipeline.create();
        int FIXED_WINDOW_SIZE = 30;

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
                        "17:00:50,Product1,10",
                        "17:00:50,Product2,40"
                ));

        // Parse the input data into TimestampedValue KVs
        PCollection<KV<String, Integer>> productSales = inputData.apply(ParDo.of(new DoFn<String, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                String[] parts = line.split(",");
                String[] timeParts = parts[0].split(":");
                LocalTime time = LocalTime.of(
                        Integer.parseInt(timeParts[0]),
                        Integer.parseInt(timeParts[1]),
                        Integer.parseInt(timeParts[2])
                );
                LocalDate date = LocalDate.now();
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

        // Sum the sales of each product in the window
        PCollection<KV<IntervalWindow, KV<String, Integer>>> sumOfProductSales = windowedProductSales
                .apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<IntervalWindow, KV<String, Integer>>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow window) {
                        String productName = c.element().getKey();
                        Iterable<Integer> sales = c.element().getValue();

                        int totalSales = 0;
                        for (int sale : sales) {
                            totalSales += sale;
                        }

                        IntervalWindow intervalWindow = (IntervalWindow) window;
                        c.output(KV.of(intervalWindow, KV.of(productName, totalSales)));
                    }
                }));

        // Group the results by window and format the output
        PCollection<String> formattedOutput = sumOfProductSales.apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<IntervalWindow, Iterable<KV<String, Integer>>>, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        IntervalWindow window = c.element().getKey();
                        Iterable<KV<String, Integer>> productSales = c.element().getValue();

                        StringBuilder sb = new StringBuilder();
                        sb.append("Window [Start: ").append(window.start()).append(" End: ").append(window.end()).append("]\n");

                        for (KV<String, Integer> sale : productSales) {
                            sb.append("\tProduct: ").append(sale.getKey()).append(", Total Sale: ").append(sale.getValue()).append("\n");
                        }

                        c.output(sb.toString());
                    }
                }));

        // Print the results
        formattedOutput.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        // Run the pipeline
        pipeline.run();
    }
}
