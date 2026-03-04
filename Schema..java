import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import java.util.*;

public class DeepJsonSchemaBuilder {

    // Step 1: Collect COMPLETE schema by scanning ALL files
    public static StructType inferCompleteSchema(SparkSession spark, String path) {
        
        // Read every file as raw text (no inference)
        Dataset<Row> raw = spark.read()
            .option("wholetext", true)   // each file = one row
            .text(path);

        // Collect all JSON strings to driver
        List<String> allJsons = raw.as(Encoders.STRING()).collectAsList();

        // Merge all schemas using Jackson
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> mergedSchema = new LinkedHashMap<>();

        for (String json : allJsons) {
            try {
                // Handle both JSON arrays and objects
                JsonNode node = mapper.readTree(json.trim());
                if (node.isArray()) {
                    for (JsonNode element : node) {
                        deepMerge(mergedSchema, mapper.convertValue(element, Map.class));
                    }
                } else {
                    deepMerge(mergedSchema, mapper.convertValue(node, Map.class));
                }
            } catch (Exception e) {
                System.err.println("Skipping malformed JSON: " + e.getMessage());
            }
        }

        return buildSparkSchema(mergedSchema);
    }

    // Deep merge two maps recursively
    @SuppressWarnings("unchecked")
    private static void deepMerge(Map<String, Object> base, Map<String, Object> override) {
        if (override == null) return;
        for (Map.Entry<String, Object> entry : override.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();

            if (!base.containsKey(key)) {
                base.put(key, val);  // new key found → add it
            } else {
                Object existing = base.get(key);
                // Both are nested maps → recurse deeper
                if (existing instanceof Map && val instanceof Map) {
                    deepMerge((Map<String, Object>) existing, (Map<String, Object>) val);
                }
                // Both are lists → merge element schemas
                else if (existing instanceof List && val instanceof List) {
                    mergeListSchemas((List<Object>) existing, (List<Object>) val);
                }
                // else: keep existing (type already captured)
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void mergeListSchemas(List<Object> base, List<Object> incoming) {
        // Find first map element in each list and merge schemas
        Map<String, Object> baseMap = findFirstMap(base);
        Map<String, Object> incomingMap = findFirstMap(incoming);
        if (baseMap != null && incomingMap != null) {
            deepMerge(baseMap, incomingMap);
        } else if (baseMap == null && incomingMap != null) {
            base.add(incomingMap);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> findFirstMap(List<Object> list) {
        for (Object item : list) {
            if (item instanceof Map) return (Map<String, Object>) item;
        }
        return null;
    }

    // Convert merged Java Map → Spark StructType
    @SuppressWarnings("unchecked")
    private static StructType buildSparkSchema(Map<String, Object> map) {
        List<StructField> fields = new ArrayList<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            fields.add(DataTypes.createStructField(key, inferDataType(val), true));
        }

        return DataTypes.createStructType(fields);
    }

    @SuppressWarnings("unchecked")
    private static DataType inferDataType(Object val) {
        if (val == null)                  return DataTypes.StringType;
        if (val instanceof Map)           return buildSparkSchema((Map<String, Object>) val);
        if (val instanceof List) {
            List<Object> list = (List<Object>) val;
            if (list.isEmpty())           return DataTypes.createArrayType(DataTypes.StringType);
            Object first = findFirstMap(list);
            if (first != null)            return DataTypes.createArrayType(buildSparkSchema((Map<String, Object>) first));
            return DataTypes.createArrayType(inferPrimitive(list.get(0)));
        }
        return inferPrimitive(val);
    }

    private static DataType inferPrimitive(Object val) {
        if (val instanceof Integer || val instanceof Long)   return DataTypes.LongType;
        if (val instanceof Double || val instanceof Float)   return DataTypes.DoubleType;
        if (val instanceof Boolean)                          return DataTypes.BooleanType;
        return DataTypes.StringType;
    }

    // ---- MAIN ----
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("DeepJsonFlattener")
            .master("local[*]")
            .getOrCreate();

        String path = "path/to/json/files/*";

        // Build complete schema from ALL files
        StructType completeSchema = inferCompleteSchema(spark, path);
        System.out.println("Complete Schema: " + completeSchema.treeString());

        // Now read with the complete schema — nothing will be missing
        Dataset<Row> df = spark.read()
            .option("multiLine", true)
            .schema(completeSchema)
            .json(path);

        df.printSchema();
        df.show(false);

        spark.stop();
    }
}
```

---

## What This Does Differently
```
File 1: user → { name, age }
File 2: user → { name, address → { city, zip } }
File 3: user → { name, orders → [{ id, items → [{ sku, qty }] }] }

Spark default mergeSchema → user: { name, age }   ❌ misses address, orders

This approach →  user: {              ✅ everything found
                   name,
                   age,
                   address: { city, zip },
                   orders: [{ id, items: [{ sku, qty }] }]
                                  }
