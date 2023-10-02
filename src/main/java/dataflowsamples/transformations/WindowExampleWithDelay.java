package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class WindowExampleWithDelay {
    public static void main(String[] args) {
        // Create a pipeline.
        Pipeline pipeline = Pipeline.create();

        // List of dummy events
        List<String> events = Arrays.asList("event1", "event2", "event3", "event4", "event5");

        // Create dummy data with a delay
        PCollection<String> input = pipeline.apply(Create.of("START"))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws InterruptedException {
                        for (String event : events) {
                            // Emit the event
                            c.output(event);
                            // Introduce a delay of 10 seconds between events
                            Thread.sleep(10000);
                        }
                    }
                }));

        // Apply a window transform to split the data into 1-minute windows.
        PCollection<String> windowedInput = input.apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
        );

        // Dummy transformation to demonstrate processing on windowed data.
        PCollection<String> processed = windowedInput.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                // Just print the element for demonstration purposes.
                System.out.println(c.element());
            }
        }));

        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }
}
