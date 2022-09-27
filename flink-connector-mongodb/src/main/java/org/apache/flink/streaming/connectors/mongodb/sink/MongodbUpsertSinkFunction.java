package org.apache.flink.streaming.connectors.mongodb.sink;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbUpsertSinkFunction extends MongodbBaseSinkFunction<RowData> {
    private final DynamicTableSink.DataStructureConverter converter;
    private final String[] fieldNames;

    public MongodbUpsertSinkFunction(MongodbSinkConf mongodbSinkConf, String[] fieldNames, DynamicTableSink.DataStructureConverter converter) {
        super(mongodbSinkConf);
        this.fieldNames = fieldNames;
        this.converter = converter;
    }

    /**
     * 将二进制RowData转换成flink可处理的Row，再将Row封装成要插入的Document对象
     *
     * @param value
     * @param context
     * @return
     */
    @Override
    WriteModel<Document> invokeDocument(RowData value, Context context) {
        Row row = (Row) this.converter.toExternal(value);
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < this.fieldNames.length; i++) {
            map.put(this.fieldNames[i], row.getField(i));
        }
        if (map.containsKey("k") && map.containsKey("hk") && map.containsKey("hv")) {
            // source: k, hk, hv
            // dest: k, v{hk,hv}
            String hk = String.format("v.%s", map.get("hk"));
            return new UpdateOneModel<Document>(new Document("k", map.get("k")),
                    new Document("$set", new Document(hk, map.get("hv"))),
                    new UpdateOptions().upsert(true));
        }
        System.out.printf("insert model..");
        return new InsertOneModel<>(new Document(map));

    }
}
