package com.latva.app.beam;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;

public class ChecksumGenerator {

  public static class GenerateChecksumsFn extends DoFn<FileIO.ReadableFile, KV<String, String>> {

    @ProcessElement
    public void process(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, String>> receiver) {
      try {
        ReadableByteChannel rbc = file.open();

        // calculating digest via a buffer
        ByteBuffer buffer = ByteBuffer.allocate(8192);

        int count;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        while ((count = rbc.read(buffer)) > 0) {
          digest.update(buffer.array(), 0, count);
          buffer.clear();
        }
        rbc.close();

        byte[] hash = digest.digest();

        // representing hash in hex
        StringBuilder hashSb = new StringBuilder();
        for (byte b : hash) {
          hashSb.append(String.format("%02x", b));
        }

        receiver.output(KV.of(file.getMetadata().resourceId().getFilename(), hashSb.toString()));
      } catch (Exception e) {
        System.out.println("Error reading file: " + e.getMessage());
      }
    }
  }

  public static class CombineChecksumsFn
      extends CombineFn<KV<String, String>, CombineChecksumsFn.Accum, Map<String, String>> {
    public static class Accum implements Serializable {
      Map<String, String> fileChecksumMap = new HashMap<String, String>();
    }

    public Accum createAccumulator() {
      return new Accum();
    }

    public Accum addInput(Accum accum, KV<String, String> input) {
      accum.fileChecksumMap.put(input.getKey(), input.getValue());
      return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = createAccumulator();
      for (Accum accum : accums) {
        merged.fileChecksumMap.putAll(accum.fileChecksumMap);
      }
      return merged;
    }

    public Map<String, String> extractOutput(Accum accum) {
      return accum.fileChecksumMap;
    }
  }

  public static class FormatAsJSONTextFn extends SimpleFunction<Map<String, String>, String> {
    @Override
    public String apply(Map<String, String> input) {
      Gson gson = new Gson();
      return gson.toJson(input);
    }
  }

  public interface ChecksumGeneratorOptions extends PipelineOptions {
    @Description("Path of the directory to read files from")
    @Default.String("/testdata")
    String getInputDirectory();

    void setInputDirectory(String value);

    @Description("Name of the file to write the manifest to")
    String getOutput();

    void setOutput(String value);
  }

  static void runChecksumGenerator(ChecksumGeneratorOptions options) {
    Pipeline p = Pipeline.create(options);

    p.apply(FileIO.match().filepattern(options.getInputDirectory() + "/*")).apply(FileIO.readMatches())
        .apply(ParDo.of(new GenerateChecksumsFn())).apply(Combine.globally(new CombineChecksumsFn()))
        .apply(MapElements.via(new FormatAsJSONTextFn())).apply(TextIO.write().to("manifest.json").withoutSharding());

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    ChecksumGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(ChecksumGeneratorOptions.class);

    runChecksumGenerator(options);
  }
}
