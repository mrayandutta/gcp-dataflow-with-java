package dataflowsamples.transformations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TemperatureConverter {
    public static void main(String[] args) {
        // 1. Initialize the Apache Beam pipeline.
        Pipeline p = Pipeline.create();

        // 2. Define input data as a PCollection.
        PCollection<Integer> celsiusTemps = p.apply(Create.of(0, 25, 100));

        // 3. Apply the Map transform to convert Celsius to Fahrenheit.
        PCollection<Integer> fahrenheitTemps = celsiusTemps.apply(
                MapElements.into(TypeDescriptors.integers()) // Specify the output type.
                        .via((Integer temp) -> (int) (temp * 9.0 / 5.0 + 32)) // Transformation function.
        );

        // 4. Output the converted temperatures.
        fahrenheitTemps.apply(ParDo.of(new DoFn<Integer, Void>() {
            @ProcessElement
            public void processElement(@Element Integer temp) {
                System.out.println(temp);
            }
        }));

        // 5. Execute the pipeline.
        p.run().waitUntilFinish();
    }
}

