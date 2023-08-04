/*
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;

/**
 * This class does a static, one-time dump of a BigQuery Table
 * from a specified dataset and puts the data (in JSON format)
 * onto the specified PubSub Topic
 */
public class BigQueryToPubSub {
    public static final Logger log = LoggerFactory.getLogger(BigQueryToPubSub.class);

    /**
     * Here we extend the default Dataflow GcpOptions
     * in order to specify:
     * * Dataset - containing the BigQuery table to read
     * * Table - the BigQuery table to read
     * * Topic - the PubSub topic to write to
     * It is assumed that they exist in the same project
     * that will run this Dataflow job
     */
    public interface BqToPubsubOptions extends GcpOptions {
        @Description("Dataset containing table to stream")
        String getDataset();

        void setDataset(String value);

        @Description("Table containing data to put into pubsub")
        String getTable();

        void setTable(String value);

        @Description("Pubsub Topic to write to")
        String getTopic();

        void setTopic(String value);
    }
    
    /**
     * The primary execution method which applies the ParDo transformations
     * * Reads the BigQuery TableRows using the Storage Read API (via withMethod(DIRECT_READ))
     * * Converts the TableRow elements into PubSub messages with the current timestamp as the message key
     * * Writes the PubSub messages to the specified topic
     * 
     * @param options extends GcpOptions
     * @return the PipelineResult object from running our BQ to PubSub pipeline
     */
    static PipelineResult runBqToPubsub(BqToPubsubOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("Read from BigQuery table",
                BigQueryIO.readTableRows()
                        .from(String.format("%s:%s.%s", options.getProject(), options.getDataset(),
                                options.getTable()))
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withCoder(TableRowJsonCoder.of()))
                .apply("Convert TableRow To PubsubMessage", ParDo.of(new DoFn<TableRow, PubsubMessage>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String gsonString = new Gson().toJson(c.element(), TableRow.class);
                        byte[] bytes = gsonString.getBytes();
                        log.debug(gsonString);

                        long timestamp = System.currentTimeMillis();
                        PubsubMessage message = new PubsubMessage(bytes,
                                Collections.singletonMap("ts", String.valueOf(timestamp)));
                        c.output(message);

                    }
                })).apply("Write to Pubsub",
                        PubsubIO.writeMessages()
                                .to(String.format("projects/%s/topics/%s", options.getProject(),
                                        options.getTopic())));

        /**
         * We've opted to run this async, rather than waiting on the Dataflow pipeline to exit
         */
        return p.run();
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(BqToPubsubOptions.class);
        BqToPubsubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BqToPubsubOptions.class);

        runBqToPubsub(options);
    }
}
