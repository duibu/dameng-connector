package org.devlive.connector.startup;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据表配置
 */
public class DataTableConfig {

    public static final JSONObject CONFIG_JSON;

    static {
        try (InputStream is = DataTableConfig.class
                             .getClassLoader()
                             .getResourceAsStream("table-config.json")) {

            if (is == null) {
                throw new IllegalStateException("table-config.json not found in classpath");
            }

            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            CONFIG_JSON = JSON.parseObject(json);

        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private DataTableConfig() {
    }
    
    public static String getDatabaseByTableName(String tableName) {
        Set<Map.Entry<String, Object>> entries = CONFIG_JSON.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            String key = entry.getKey();
            JSONObject value = JSONObject.from(entry.getValue());
            JSONArray tableNameArrays = value.getJSONArray("table_list");
            List<String> tableNameList = tableNameArrays.stream().map(JSONObject::from).map(a -> a.getString("table_name")).toList();
            if (tableNameList.contains(tableName)) {
                return key;
            }
        }
        return "";
    }

    public static void main(String[] args) {
    }
    
    
    
}
