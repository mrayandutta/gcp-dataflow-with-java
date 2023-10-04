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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class FixedWindowSample {

    public static void main(String[] args) {
        // Create a pipeline
        Pipeline pipeline = Pipeline.create();
        int FIXED_WINDOW_SIZE = 30;

        // Create a PCollection of input data with dates included
        PCollection<String> inputData = pipeline
                .apply(Create.of(
                        "2023-10-01T17:00:00,Product1,10",
                        "2023-10-01T17:10:00,Product1,10",
                        "2023-10-01T17:10:00,Product2,40",
                        "2023-10-01T17:20:00,Product2,10",
                        "2023-10-01T17:30:00,Product1,10",
                        "2023-10-01T17:30:00,Product2,40",
                        "2023-10-01T17:40:00,Product2,100",
                        "2023-10-01T17:40:00,Product1,100"
                ));


        // Parse the input data into TimestampedValue KVs
        PCollection<KV<String, Integer>> productSales = inputData.apply(ParDo.of(new DoFn<String, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                String[] parts = line.split(",");

                LocalDateTime dateTime = LocalDateTime.parse(parts[0], java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                long epochTime = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

                c.outputWithTimestamp(
                        KV.of(parts[1], Integer.parseInt(parts[2])),
                        Instant.ofEpochMilli(epochTime)
                );
            }
        }));

        PCollection<KV<String, Integer>> windowedProductSales = productSales.apply(
                Window.into(FixedWindows.of(Duration.standardMinutes(FIXED_WINDOW_SIZE)))
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
                        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

                        StringBuilder sb = new StringBuilder();
                        sb.append("Window [Start: ").append(formatter.print(window.start())).append(" End: ").append(formatter.print(window.end())).append("]\n");

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
