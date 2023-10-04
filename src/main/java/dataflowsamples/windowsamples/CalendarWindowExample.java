package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

public class CalendarWindowExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> input = pipeline.apply(Create.of(
                "2023-10-02T19:00:00,ProductA,2",
                "2023-10-02T20:00:00,ProductA,1",
                "2023-10-02T22:30:00,ProductB,1",
                "2023-10-02T23:30:00,ProductA,1",
                "2023-10-03T01:30:00,ProductB,1",
                "2023-10-03T03:00:00,ProductA,2",
                "2023-10-04T05:00:00,ProductB,1"
        ));

        PCollection<KV<String, Integer>> productSales = input.apply(ParDo.of(new ExtractSalesFn()));

        PCollection<KV<String, Integer>> windowedProductSales = productSales
                .apply(Window.into(new DailyWindowFn(Duration.standardDays(1))));

        PCollection<KV<String, KV<String, Integer>>> byWindow = windowedProductSales
                .apply(ParDo.of(new AssignToWindowFn()));

        PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedByWindow = byWindow.apply(GroupByKey.create());

        PCollection<String> result = groupedByWindow.apply(ParDo.of(new FormatOutputFn()));

        result.apply(ParDo.of(new PrintFn()));

        pipeline.run();
    }

    public static class ExtractSalesFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] parts = c.element().split(",");
            Instant timestamp = Instant.parse(parts[0] + "Z");
            String product = parts[1];
            int count = Integer.parseInt(parts[2]);
            c.outputWithTimestamp(KV.of(product, count), timestamp);
        }
    }

    public static class AssignToWindowFn extends DoFn<KV<String, Integer>, KV<String, KV<String, Integer>>> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            String intervalStr = intervalWindow.start().toString() + "_" + intervalWindow.end().toString();
            c.output(KV.of(intervalStr, c.element()));
        }
    }

    public static class FormatOutputFn extends DoFn<KV<String, Iterable<KV<String, Integer>>>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String windowStr = c.element().getKey();
            Iterable<KV<String, Integer>> salesData = c.element().getValue();

            Map<String, Integer> combinedSales = new HashMap<>();
            for (KV<String, Integer> sale : salesData) {
                String product = sale.getKey();
                combinedSales.put(product, combinedSales.getOrDefault(product, 0) + sale.getValue());
            }

            StringBuilder output = new StringBuilder();
            output.append("Day: ").append(windowStr).append("\n");
            for (Map.Entry<String, Integer> entry : combinedSales.entrySet()) {
                output.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }

            c.output(output.toString());
        }
    }

    public static class PrintFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }

    public static class DailyWindowFn extends PartitioningWindowFn<Object, IntervalWindow> {
        private static final long serialVersionUID = 0L;
        private final Duration size;

        public DailyWindowFn(Duration size) {
            this.size = size;
        }

        @Override
        public IntervalWindow assignWindow(Instant timestamp) {
            Instant startOfDay = new Instant(timestamp.getMillis() - timestamp.getMillis() % size.getMillis());
            return new IntervalWindow(startOfDay, size);
        }

        @Override
        public boolean isCompatible(WindowFn<?, ?> other) {
            return other instanceof DailyWindowFn;
        }

        @Override
        public Coder<IntervalWindow> windowCoder() {
            return IntervalWindow.getCoder();
        }

        @Override
        public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
            throw new UnsupportedOperationException("This WindowFn does not support side inputs");
        }
    }
}
