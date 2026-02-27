package org.devlive.connector.startup;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ChangeEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventProcessor.class);

    private final boolean isEncrypt;

    public ChangeEventProcessor(boolean isEncrypt) {
        this.isEncrypt = isEncrypt;
    }

    /**
     * 处理变更事件
     *
     * @param key CDC 事件的 key
     * @param value CDC 事件的 value
     * @return 处理结果，如果不需要发送则返回 null
     */
    public EventProcessResult process(String key, String value) {
        try {
            LOGGER.debug("key: {}", key);
            LOGGER.debug("value: {}", value);
            LOGGER.debug("Received change event: {}", value);

            // 解析并处理 key
            JSONObject recordKey = JSONObject.parseObject(key);
            JSONObject keySchemaMap = buildSchemaTypeMap(recordKey.getJSONObject("schema"));
            String processedKey = processEncryptedKey(recordKey, keySchemaMap);

            // 提取主键字段名
            String primaryKey = extractPrimaryKeyField(recordKey);
            if (StringUtils.isBlank(primaryKey)) {
                LOGGER.warn("Primary key is null, skipping record");
                return null;
            }

            // 处理 value
            ValueProcessResult valueResult = processChangeValue(value, primaryKey);
            if (!valueResult.canSend) {
                return null;
            }

            System.out.println(processedKey);
            System.out.println(valueResult.message);

            // 构建返回结果
            if (StringUtils.isNotBlank(valueResult.targetDatabase)) {
                EventProcessResult result = new EventProcessResult();
                result.key = processedKey;
                result.value = valueResult.message;
                result.targetDatabase = valueResult.targetDatabase;
                return result;
            }

            return null;

        } catch (Exception e) {
            LOGGER.error("Error processing change event", e);
            return null;
        }
    }

    /**
     * 构建字段类型映射
     */
    private JSONObject buildSchemaTypeMap(JSONObject schema) {
        JSONObject typeMap = new JSONObject();
        if (schema != null && schema.containsKey("fields")) {
            JSONArray fields = schema.getJSONArray("fields");
            for (Object item : fields) {
                JSONObject field = (JSONObject) item;
                typeMap.put(field.getString("field"), field.get("type"));
            }
        }
        return typeMap;
    }

    /**
     * 处理加密的 key
     */
    private String processEncryptedKey(JSONObject recordKey, JSONObject keySchemaMap) {
        boolean keyChanged = false;
        JSONObject payload = recordKey.getJSONObject("payload");

        if (payload != null) {
            for (String fieldName : payload.keySet()) {
                if (shouldDecryptField(fieldName, payload.getString(fieldName), keySchemaMap)) {
                    payload.put(fieldName, CaesarEncryptUtils.decryptCaesarSalt(payload.getString(fieldName), ""));
                    keyChanged = true;
                }
            }
        }

        return recordKey.toJSONString(JSONWriter.Feature.WriteMapNullValue);
    }

    /**
     * 提取主键字段名
     */
    private String extractPrimaryKeyField(JSONObject recordKey) {
        JSONObject schema = recordKey.getJSONObject("schema");
        if (schema == null) {
            return null;
        }

        JSONArray fields = schema.getJSONArray("fields");
        if (fields != null && !fields.isEmpty()) {
            return fields.getJSONObject(0).getString("field");
        }
        return null;
    }

    /**
     * 处理变更值
     */
    private ValueProcessResult processChangeValue(String value, String primaryKey) {
        ValueProcessResult result = new ValueProcessResult();

        if (value == null) {
            result.message = null;
            result.canSend = true;
            return result;
        }

        JSONObject recordValue = JSONObject.parseObject(value);
        JSONObject payload = recordValue.getJSONObject("payload");

        if (payload == null) {
            LOGGER.warn("Payload is null, skipping record");
            result.canSend = false;
            return result;
        }

        JSONObject after = payload.getJSONObject("after");
        JSONObject before = payload.getJSONObject("before");
        JSONObject source = payload.getJSONObject("source");

        if (after == null || source == null) {
            LOGGER.debug("No 'after' or 'source' data, skipping");
            result.canSend = false;
            return result;
        }

        // 构建 value 的字段类型映射
        JSONObject valueSchemaMap = buildValueSchemaTypeMap(recordValue.getJSONObject("schema"));

        // 获取主键值
        String primaryKeyValue = extractPrimaryKeyValue(after, before, primaryKey);

        boolean isChanged = false;

        // 处理 before 数据解密
        if (before != null) {
            isChanged |= decryptFields(before, valueSchemaMap);
        }

        // 处理 after 数据
        FieldProcessResult fieldResult = processAfterFields(after, source, primaryKey,
                primaryKeyValue, valueSchemaMap);
        isChanged |= fieldResult.isChanged;
        result.canSend = fieldResult.canSend;

        if (!result.canSend) {
            return result;
        }

        // 处理数据库映射
        String sourceTableName = source.getString("table");
        String targetDatabase = DataTableConfig.getDatabaseByTableName(sourceTableName.toLowerCase());

        if (StringUtils.isNotBlank(targetDatabase)) {
            isChanged = true;
            source.put("schema", targetDatabase);
            source.put("db", targetDatabase);
            result.targetDatabase = targetDatabase;
        } else {
            result.canSend = false;
        }

        result.message = isChanged ? recordValue.toJSONString(JSONWriter.Feature.WriteMapNullValue) : value;
        return result;
    }

    /**
     * 构建 value 的字段类型映射（针对 after 结构）
     */
    private JSONObject buildValueSchemaTypeMap(JSONObject schema) {
        JSONObject typeMap = new JSONObject();
        if (schema == null || !schema.containsKey("fields")) {
            return typeMap;
        }

        JSONArray fields = schema.getJSONArray("fields");
        for (Object item : fields) {
            JSONObject field = (JSONObject) item;
            if ("struct".equalsIgnoreCase(field.getString("type"))
                    && "after".equalsIgnoreCase(field.getString("field"))) {
                JSONArray innerFields = field.getJSONArray("fields");
                if (innerFields != null) {
                    for (Object inner : innerFields) {
                        JSONObject innerField = (JSONObject) inner;
                        typeMap.put(innerField.getString("field"), innerField.get("type"));
                    }
                }
                break;
            }
        }
        return typeMap;
    }

    /**
     * 提取主键值
     */
    private String extractPrimaryKeyValue(JSONObject after, JSONObject before, String primaryKey) {
        String keyValue = null;
        if (after != null) {
            keyValue = after.getString(primaryKey);
        }
        if (StringUtils.isBlank(keyValue) && before != null) {
            keyValue = before.getString(primaryKey);
        }
        return keyValue;
    }

    /**
     * 解密字段
     */
    private boolean decryptFields(JSONObject data, JSONObject schemaMap) {
        boolean changed = false;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldKey = entry.getKey();
            Object fieldValue = entry.getValue();

            if (shouldDecryptField(fieldKey, fieldValue, schemaMap)) {
                data.put(fieldKey, CaesarEncryptUtils.decryptCaesarSalt(fieldValue.toString(), ""));
                changed = true;
            }
        }
        return changed;
    }

    /**
     * 判断字段是否需要解密
     */
    private boolean shouldDecryptField(String fieldKey, Object fieldValue, JSONObject schemaMap) {
        if (!isEncrypt || fieldKey == null || fieldValue == null) {
            return false;
        }

        String valueStr = fieldValue.toString();
        if (StringUtils.isBlank(valueStr) || StringUtils.isNumeric(valueStr)) {
            return false;
        }

        if (fieldKey.toLowerCase().contains("time")) {
            return false;
        }

        return "string".equalsIgnoreCase(schemaMap.getString(fieldKey));
    }

    /**
     * 处理 after 字段
     */
    private FieldProcessResult processAfterFields(JSONObject after, JSONObject source,
                                                  String primaryKey, String primaryKeyValue,
                                                  JSONObject schemaMap) {
        FieldProcessResult result = new FieldProcessResult();
        result.canSend = true;
        result.isChanged = false;

        for (Map.Entry<String, Object> entry : after.entrySet()) {
            String fieldKey = entry.getKey();
            Object fieldValue = entry.getValue();

            // 跳过主键为 null 的情况
            if (primaryKey.equalsIgnoreCase(fieldKey) && fieldValue == null) {
                continue;
            }

            // 处理 CLOB 字段
            if (isClobPlaceholder(fieldValue)) {
                if (!queryClobField(after, source, primaryKey, primaryKeyValue, fieldKey)) {
                    result.canSend = false;
                }
                result.isChanged = true;
                continue;
            }

            // 处理空字段（包括 ID 字段）
            if (fieldValue == null || StringUtils.isBlank(fieldValue.toString())) {
                if (!queryMissingField(after, source, primaryKey, primaryKeyValue, fieldKey)) {
                    result.canSend = false;
                }
                result.isChanged = true;
                continue;
            }

            // 处理加密字段
            if (shouldDecryptField(fieldKey, fieldValue, schemaMap)) {
                after.put(fieldKey, CaesarEncryptUtils.decryptCaesarSalt(fieldValue.toString(), ""));
            }
        }

        return result;
    }

    /**
     * 判断是否为 CLOB 占位符
     */
    private boolean isClobPlaceholder(Object fieldValue) {
        if (fieldValue == null) {
            return false;
        }
        String valueStr = String.valueOf(fieldValue).toUpperCase();
        return "OUT_CLOB".equals(valueStr) || valueStr.startsWith("OUT_CLOB");
    }

    /**
     * 查询 CLOB 字段值
     */
    private boolean queryClobField(JSONObject after, JSONObject source, String primaryKey,
                                   String primaryKeyValue, String fieldKey) {
        if (StringUtils.isBlank(primaryKeyValue)) {
            LOGGER.warn("primaryKey is null or empty, cannot query CLOB");
            return false;
        }

        String sql = String.format(
                "SELECT %s FROM %s.%s WHERE %s = ?",
                fieldKey,
                source.getString("schema"),
                source.getString("table"),
                primaryKey
        );

        try {
            Map<String, Object> result = JdbcQueryUtils.queryOne(sql, primaryKeyValue);
            if (result != null && result.containsKey(fieldKey)) {
                after.put(fieldKey, result.get(fieldKey));
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query CLOB field: {}", fieldKey, e);
            return false;
        }
        return true;
    }

    /**
     * 查询缺失字段值
     */
    private boolean queryMissingField(JSONObject after, JSONObject source, String primaryKey,
                                      String primaryKeyValue, String fieldKey) {
        if (StringUtils.isBlank(primaryKeyValue)) {
            return true;
        }

        String sql = String.format(
                "SELECT %s FROM %s.%s WHERE %s = ?",
                fieldKey,
                source.getString("schema"),
                source.getString("table"),
                primaryKey
        );

        try {
            Map<String, Object> result = JdbcQueryUtils.queryOne(sql, primaryKeyValue);
            if (result != null && result.get(fieldKey) != null) {
                after.put(fieldKey, result.get(fieldKey).toString().trim());
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to query field: {}", fieldKey, e);
            return false;
        }
    }

    /**
     * 事件处理结果
     */
    public static class EventProcessResult {
        private String key;
        private String value;
        private String targetDatabase;

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public String getTargetDatabase() {
            return targetDatabase;
        }

        public String getTopic() {
            return "data-sync-dm-to-mysql." + targetDatabase;
        }
    }

    /**
     * Value 处理结果（内部使用）
     */
    private static class ValueProcessResult {
        String message;
        boolean canSend = true;
        String targetDatabase;
    }

    /**
     * 字段处理结果（内部使用）
     */
    private static class FieldProcessResult {
        boolean isChanged;
        boolean canSend;
    }
    
}
