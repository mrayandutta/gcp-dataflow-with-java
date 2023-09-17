package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class PositiveReviewFilter {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Sample product reviews
        p.apply(Create.of("This product is absolutely amazing!",
                        "It's okay, but I've seen better.",
                        "Fantastic quality and design!",
                        "I did not like this product at all."))
                // Applying the Filter transformation
                .apply("FilterPositiveReviews", Filter.by(review -> review.contains("amazing") || review.contains("Fantastic")))
                .apply("PrintReviews", ParDo.of(new PrintReviewFn()));

        p.run().waitUntilFinish();
    }

    static class PrintReviewFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String review) {
            // Printing out the filtered positive reviews
            System.out.println("Positive Review: " + review);
        }
    }
}

