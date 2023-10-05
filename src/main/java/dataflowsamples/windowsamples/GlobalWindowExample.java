package dataflowsamples.windowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GlobalWindowExample {

    public static void main(String[] args) {
        // Create a pipeline
        Pipeline pipeline = Pipeline.create();

        // Sample e-commerce purchase data: "date,time,user,action,purchaseAmount"
        PCollection<String> inputData = pipeline.apply(Create.of(
                "2023-10-04,09:00:00,User1,BuyLaptop,1200",
                "2023-10-05,14:30:00,User1,BuyHeadphones,50",
                "2023-10-04,11:15:00,User2,BuyMobilePhone,700"
        ));

        // Parse input data to TimestampedValue
        PCollection<KV<String, Double>> userPurchases = inputData.apply(ParDo.of(new DoFn<String, KV<String, Double>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] parts = c.element().split(",");
                String user = parts[2];
                Double purchaseAmount = Double.valueOf(parts[4]);
                c.output(KV.of(user, purchaseAmount));
            }
        }));

        // Group purchases by user in a global window
        PCollection<KV<String, Iterable<Double>>> globalPurchases = userPurchases.apply(GroupByKey.create());

        // Calculate LTV for each user
        PCollection<KV<String, Double>> userLTV = globalPurchases.apply(ParDo.of(new DoFn<KV<String, Iterable<Double>>, KV<String, Double>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String user = c.element().getKey();
                Iterable<Double> purchases = c.element().getValue();
                double totalSpend = 0.0;
                for (Double purchase : purchases) {
                    totalSpend += purchase;
                }
                c.output(KV.of(user, totalSpend));
            }
        }));

        // Print LTV for each user
        userLTV.apply(ParDo.of(new DoFn<KV<String, Double>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("LTV for " + c.element().getKey() + ": $" + c.element().getValue());
            }
        }));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
