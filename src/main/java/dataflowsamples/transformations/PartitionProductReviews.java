package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class PartitionProductReviews {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Sample product reviews with ratings
        PCollection<Double> reviews = p.apply(Create.of(4.5, 3.8, 5.0, 4.2, 3.6));

        // Applying the Partition transform
        int numPartitions = 2;
        PCollectionList<Double> partitionedReviews = reviews.apply(
                Partition.of(numPartitions, new Partition.PartitionFn<Double>() {
                    public int partitionFor(Double elem, int numPartitions) {
                        return (elem >= 4.0) ? 0 : 1; // 0 for positive reviews, 1 for others
                    }
                }));

        // Printing each partition
        partitionedReviews.get(0).apply("PrintPositiveReviews", ParDo.of(new PrintFn("Positive Review")));
        partitionedReviews.get(1).apply("PrintOtherReviews", ParDo.of(new PrintFn("Other Review")));

        p.run().waitUntilFinish();
    }

    static class PrintFn extends DoFn<Double, Void> {
        private String type;

        PrintFn(String type) {
            this.type = type;
        }

        @ProcessElement
        public void processElement(@Element Double review) {
            // Printing out the review based on its type
            System.out.println(type + ": " + review);
        }
    }
}

