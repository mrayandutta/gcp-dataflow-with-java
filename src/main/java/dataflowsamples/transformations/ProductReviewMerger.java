package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class ProductReviewMerger {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> reviewsFromSourceA = p.apply("SourceAReviews", Create.of("Excellent", "Very good"));
        PCollection<String> reviewsFromSourceB = p.apply("SourceBReviews", Create.of("Average", "Good"));

        PCollection<String> allReviews = PCollectionList.of(reviewsFromSourceA)
                .and(reviewsFromSourceB)
                .apply(Flatten.pCollections());

        allReviews.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(@Element String review) {
                System.out.println(review);
            }
        }));

        p.run().waitUntilFinish();
    }
}
