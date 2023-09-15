package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class UppercaseConverter {

    /**
     * A Beam DoFn that converts input words into uppercase.
     */
    public static class ConvertToUpperFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String word = c.element();
            c.output(word.toUpperCase());
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> words = p.apply(Create.of("Hello", "Beam", "World"));
        PCollection<String> uppercasedWords = words.apply(ParDo.of(new ConvertToUpperFn()));

        // Here, simply printing out the results.
        uppercasedWords.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        p.run().waitUntilFinish();
    }
}