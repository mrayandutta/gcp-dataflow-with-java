package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.LocalDate;

public class SessionWindowExample {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Define gap duration
        Duration gapDuration = Duration.standardMinutes(15);

        // Sample shopping app data
        PCollection<String> inputData = pipeline.apply(Create.of(
                "09:00:00,User1,ViewLaptop",
                "09:03:00,User1,AddToCartLaptop",
                "09:10:00,User1,Checkout",
                "14:30:00,User1,ViewMobilePhone"
        ));

        PCollection<KV<String, String>> userActions = inputData.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] parts = c.element().split(",");
                String time = parts[0];
                String currentDate = LocalDate.now().toString();
                Instant timestamp = Instant.parse(currentDate + "T" + time + ".000Z");
                c.outputWithTimestamp(KV.of(parts[1], parts[2]), timestamp);
            }
        }));

        PCollection<KV<String, Iterable<String>>> sessionedActions = userActions
                .apply(Window.into(Sessions.withGapDuration(gapDuration)))
                .apply(GroupByKey.create());

        sessionedActions.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
                String user = c.element().getKey();
                Iterable<String> actions = c.element().getValue();
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
                System.out.println("Window for " + user + " [Start: " + formatter.print(((IntervalWindow) window).start()) + " End: " + formatter.print(((IntervalWindow) window).end()) + "]");
                for (String action : actions) {
                    System.out.println("\t- " + action);
                }
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
