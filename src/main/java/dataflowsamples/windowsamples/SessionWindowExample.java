package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class SessionWindowExample {

    public static void main(String[] args) {
        // Create a pipeline
        Pipeline pipeline = Pipeline.create();

        // Define a gap duration for session windows
        Duration gapDuration = Duration.standardMinutes(15);

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
                String currentDate = LocalDate.now().toString(); // Get current date in "YYYY-MM-DD" format
                Instant timestamp = Instant.parse(currentDate + "T" + time + ".000Z");
                c.outputWithTimestamp(KV.of(parts[1], parts[2]), timestamp);
            }
        }));

        // Apply session windows
        PCollection<KV<String, Iterable<String>>> sessionedActions = userActions
                .apply(Window.into(Sessions.withGapDuration(gapDuration)))
                .apply(GroupByKey.create());

        // Print sessions and actions
        sessionedActions.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
                String user = c.element().getKey();
                Iterable<String> actions = c.element().getValue();
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
                System.out.println("Window for " + user + " [Start: " + formatter.print(((IntervalWindow) window).start()) + " End: " + formatter.print(((IntervalWindow) window).end()) + "]");
                List<String> uniqueActions = new ArrayList<>();
                for (String action : actions) {
                    if (!uniqueActions.contains(action)) {
                        uniqueActions.add(action);
                        System.out.println("\t- " + action);
                    }
                }
            }
        }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
