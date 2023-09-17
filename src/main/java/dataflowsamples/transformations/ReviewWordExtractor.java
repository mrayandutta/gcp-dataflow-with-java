package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;

public class ReviewWordExtractor {
    public static void main(String[] args) {
        // 1. Initialize the Beam pipeline.
        Pipeline p = Pipeline.create();

        // 2. Define a source PCollection of user reviews.
        PCollection<String> reviews = p.apply(Create.of(Arrays.asList("Excellent product", "Works well for the price")));

        // 3. Apply the FlatMap transform: Split each review into individual words.
        PCollection<String> words = reviews.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via((review) -> Arrays.asList(review.split(" "))) // Splitting by spaces.
        );

        // 4. Use ParDo to print the extracted words.
        words.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(@Element String word) {
                System.out.println(word);
            }
        }));

        // 5. Run the Beam pipeline.
        p.run().waitUntilFinish();
    }
}


