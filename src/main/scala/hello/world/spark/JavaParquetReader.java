package hello.world.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class JavaParquetReader {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        Dataset<Row> df= spark.read().load("data/part-00000-95bbfcbb-ffbe-4112-8e69-876234860dd4-c000.snappy.parquet");
        StructType st = df.schema();



    }
}
