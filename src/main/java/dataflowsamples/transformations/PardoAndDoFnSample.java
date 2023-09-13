package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


public class PardoAndDoFnSample {
    public static class ComputeWordLengthFn extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().length());
        }
    }

    public static class CapitalizeWordLengthFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toUpperCase());
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> words = pipeline.apply(Create.of("Hello", "Beam", "Java"));
        PCollection<String> capitalizedWords = words.apply(ParDo.of(new CapitalizeWordLengthFn()));
        capitalizedWords.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
        PCollection<Integer> wordLengths = capitalizedWords.apply(ParDo.of(new ComputeWordLengthFn()));

        wordLengths.apply(ParDo.of(new DoFn<Integer, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }

}
