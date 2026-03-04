import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import java.util.*;

public class JsonFlattener {

    public static Dataset<Row> flattenJson(Dataset<Row> df) {
        return flattenRecursive(df, "", df.schema());
    }

    private static Dataset<Row> flattenRecursive(Dataset<Row> df, String prefix, StructType schema) {
        // Step 1: Expand all StructType fields (no explode needed, just dot-notation access)
        df = expandStructs(df, prefix, schema);

        // Step 2: Check if any ArrayType columns remain
        boolean hasArrays = Arrays.stream(df.schema().fields())
                .anyMatch(f -> f.dataType() instanceof ArrayType);

        if (!hasArrays) return df;

        // Step 3: Explode the FIRST array column found, then recurse
        for (StructField field : df.schema().fields()) {
            if (field.dataType() instanceof ArrayType) {
                String colName = field.name();
                ArrayType arrayType = (ArrayType) field.dataType();

                // Explode the array
                df = df.withColumn(colName, functions.explode_outer(df.col(colName)));

                // If the array contained structs, expand those too
                if (arrayType.elementType() instanceof StructType) {
                    df = expandStructs(df, "", df.schema());
                }

                // Recurse to handle remaining arrays
                return flattenRecursive(df, "", df.schema());
            }
        }

        return df;
    }

    /**
     * Expands all StructType columns into flat dot-notation columns.
     * e.g. address.city, address.zip
     */
    private static Dataset<Row> expandStructs(Dataset<Row> df, String prefix, StructType schema) {
        List<Column> columns = new ArrayList<>();

        for (StructField field : schema.fields()) {
            String fullName = prefix.isEmpty() ? field.name() : prefix + "_" + field.name();
            Column col = df.col("`" + field.name() + "`");

            if (field.dataType() instanceof StructType) {
                // Inline-expand struct fields
                StructType nestedSchema = (StructType) field.dataType();
                for (StructField nestedField : nestedSchema.fields()) {
                    String nestedName = fullName + "_" + nestedField.name();
                    columns.add(col.getField(nestedField.name()).alias(nestedName));
                }
            } else {
                columns.add(col.alias(fullName));
            }
        }

        return df.select(columns.toArray(new Column[0]));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JsonFlattener")
                .master("local[*]")
                .getOrCreate();

        // Read deeply nested JSON
        Dataset<Row> df = spark.read()
                .option("multiLine", true)
                .json("path/to/complex.json");

        System.out.println("=== ORIGINAL SCHEMA ===");
        df.printSchema();

        Dataset<Row> flattened = flattenJson(df);

        System.out.println("=== FLATTENED SCHEMA ===");
        flattened.printSchema();

        flattened.show(false);

        spark.stop();
    }
}
