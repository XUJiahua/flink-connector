package org.apache.flink.streaming.connectors.mongodb.sink;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public abstract class MongodbBaseSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private final MongodbSinkConf mongodbSinkConf;
    private transient MongoClient client;
    private transient List<WriteModel<Document>> batch;

    protected MongodbBaseSinkFunction(MongodbSinkConf mongodbSinkConf) {
        this.mongodbSinkConf = mongodbSinkConf;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.client = new MongoClient(new MongoClientURI(this.mongodbSinkConf.getUri(), getOptions(this.mongodbSinkConf.getMaxConnectionIdleTime())));
        this.batch = new ArrayList<WriteModel<Document>>();
    }

    private MongoClientOptions.Builder getOptions(int maxConnectionIdleTime) {
        MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
        optionsBuilder.maxConnectionIdleTime(maxConnectionIdleTime);
        return optionsBuilder;
    }

    @Override
    public void close() throws Exception {
        flush();
        super.close();
        this.client.close();
        this.client = null;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        this.batch.add(invokeDocument(value, context));
        if (this.batch.size() >= this.mongodbSinkConf.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    private void flush() {
        if (this.batch.isEmpty()) {
            return;
        }
        MongoDatabase mongoDatabase = this.client.getDatabase(this.mongodbSinkConf.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(this.mongodbSinkConf.getCollection());
        try {
            BulkWriteResult result = mongoCollection.bulkWrite(this.batch);
            System.out.println("Bulk Result statistics:" +
                    "\ninserted: " + result.getInsertedCount() +
                    "\nupdated: " + result.getModifiedCount() +
                    "\ndeleted: " + result.getDeletedCount());
        } catch (MongoException me) {
            System.err.println("The bulk write operation failed due to an error: " + me);
        }
        this.batch.clear();
    }

    abstract WriteModel<Document> invokeDocument(IN paramIN, Context paramContext) throws Exception;
}
