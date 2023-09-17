package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ReviewAggregator {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<KV<String, String>> reviews = p.apply(Create.of(KV.of("Product A", "Great"), KV.of("Product A", "Average"), KV.of("Product B", "Okay"), KV.of("Product B", "Good")));

        PCollection<KV<String, Iterable<String>>> aggregatedReviews = reviews.apply(GroupByKey.create());

        aggregatedReviews.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<String, Iterable<String>> kv) {
                System.out.println(kv.getKey() + " : " + kv.getValue());
            }
        }));

        p.run().waitUntilFinish();
    }
}
