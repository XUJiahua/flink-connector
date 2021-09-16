package org.apache.flink.streaming.connectors.mongodb.util;

import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class ContextUtil {
    public static void transformContext(DynamicTableFactory factory, DynamicTableFactory.Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();

        Map<String, String> convertedOptions = FactoryOptionUtil.normalizeOptionCaseAsFactory(factory, catalogOptions);

        catalogOptions.clear();
        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }
}
