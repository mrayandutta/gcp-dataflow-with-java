package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class AverageProductRating {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Sample product ratings
        PCollection<Double> productRatings = p.apply(Create.of(4.5, 3.8, 5.0, 4.2, 3.6));

        // Applying the Combine transformation to compute the average rating
        PCollection<Double> averageRating = productRatings.apply("ComputeAverageRating", Mean.globally());

        averageRating.apply("PrintAverageRating", ParDo.of(new PrintFn()));

        p.run().waitUntilFinish();
    }

    static class PrintFn extends DoFn<Double, Void> {
        @ProcessElement
        public void processElement(@Element Double avgRating) {
            // Printing out the computed average rating
            System.out.println("Average Product Rating: " + avgRating);
        }
    }
}
