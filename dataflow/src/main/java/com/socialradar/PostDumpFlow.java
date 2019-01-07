package com.socialradar;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


// This Java class is for Google Dataflow to get data from Google BigTable and dump them into BigQuery for offline analysis.
// As of now, Dataflow does not have GOlang API support so Java (or Python) is needed to use it.
public class PostDumpFlow {
    private static final String PROJECT_ID = "socialradar";
    private static final String BT_INSTANCE_ID = "socialradar-post";
    private static final String BT_TABLE_ID = "post";
    private static final String BQ_DATASET_ID = "post_analysis";
    private static final String BQ_TABLE_NAME = "daily_dump_1";
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
            
    public static void main(String[] args) {
        // Start by defining the options for the pipeline.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline p = Pipeline.create(options);

        // Interact with specified BigTable.
        CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
            .withProjectId(PROJECT_ID)
            .withInstanceId(BT_INSTANCE_ID)
            .withTableId(BT_TABLE_ID)
            .build();

        // Get result from BigTable (one row of result is one row of data in BigTable).
        PCollection<Result> btRows = p.apply(Read.from(CloudBigtableIO.read(config)));

        // Transform each row of BigTable data to rows of data in BigQuery. 
        PCollection<TableRow> bqRows = btRows.apply(ParDo.of(new DoFn<Result, TableRow>() {
            @Override
            public void processElement(ProcessContext c) {
                Result result = c.element();
                String postId = new String(result.getRow());
                String user = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("user")), UTF8_CHARSET);
                String message = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("message")), UTF8_CHARSET);
                String lat = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lat")), UTF8_CHARSET);
                String lon = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lon")), UTF8_CHARSET);
                TableRow row = new TableRow();//BQ Table row object.
                row.set("postId", postId);
                row.set("user", user);
                row.set("message", message);
                row.set("lat", Double.parseDouble(lat));
                row.set("lon", Double.parseDouble(lon));
                c.output(row);
            }
        }));

        // Create schema for BigQuery since it is a relational database.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("postId").setType("STRING"));  // define various columns.
        fields.add(new TableFieldSchema().setName("user").setType("STRING"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lat").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("lon").setType("FLOAT"));

        TableSchema schema = new TableSchema().setFields(fields);

        // Write data into BigQuery.
        bqRows.apply(BigQueryIO.Write
            .named("Write")
            .to(PROJECT_ID + ":" + BQ_DATASET_ID + "." + BQ_TABLE_NAME)
            .withSchema(schema)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        
        p.run();
    }
}