package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

public class ProductReviewAnalysis {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Sample data: product reviews and ratings.
        PCollection<KV<String, String>> reviews = p.apply("ProductReviews", Create.of(KV.of("Product A", "Excellent"), KV.of("Product B", "Very good")));
        PCollection<KV<String, Integer>> ratings = p.apply("ProductRatings", Create.of(KV.of("Product A", 5), KV.of("Product B", 4)));

        // Key: Product Name; Values: Product Reviews and Ratings
        PCollection<KV<String, CoGbkResult>> groupedData = KeyedPCollectionTuple
                .of("Reviews", reviews)
                .and("Ratings", ratings)
                .apply(CoGroupByKey.create());

        groupedData.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<String, CoGbkResult> kv) {
                String product = kv.getKey();
                Iterable<String> productReviews = kv.getValue().getAll("Reviews");
                Iterable<Integer> productRatings = kv.getValue().getAll("Ratings");

                System.out.println("Product: " + product);
                System.out.println("Reviews: " + productReviews);
                System.out.println("Ratings: " + productRatings);
            }
        }));

        p.run().waitUntilFinish();
    }
}
