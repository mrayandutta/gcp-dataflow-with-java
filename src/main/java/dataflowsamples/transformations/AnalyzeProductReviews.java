package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class AnalyzeProductReviews {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Sample product reviews
        p.apply(Create.of("This product is absolutely amazing!",
                        "It's okay, but I've seen better.",
                        "I did not like this product at all."))
                .apply(ParDo.of(new ReviewAnalysisFn()))
                .apply(ParDo.of(new PrintReviewAnalysisFn()));

        p.run().waitUntilFinish();
    }

    static class ReviewAnalysisFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String review = c.element();
            int wordCount = review.split(" ").length;
            String sentiment = "Neutral";

            if (wordCount > 5 && (review.contains("amazing") || review.contains("great"))) {
                sentiment = "Positive";
            } else if (review.contains("not like") || review.contains("hate")) {
                sentiment = "Negative";
            }

            c.output("Review: \"" + review + "\" | Word Count: " + wordCount + " | Sentiment: " + sentiment);
        }
    }

    static class PrintReviewAnalysisFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String analyzedReview) {
            System.out.println(analyzedReview);
        }
    }
}
