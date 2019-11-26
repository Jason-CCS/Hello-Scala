package hello.world.spark;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class JavaParquetReader {

    private static String outputDataPath = System.getProperty("user.dir") + System.getProperty("file.separator") + "output-data" +
            System.getProperty("file.separator");
    private static String inputDataPath = System.getProperty("user.dir") + System.getProperty("file.separator") + "input-data" +
            System.getProperty("file.separator");
    private static String configPath = System.getProperty("user.dir") + System.getProperty("file.separator") + "config" +
            System.getProperty("file.separator");

    public static void main(String[] args) {
        List<Row> rowList = ParquetReader.getAsRowList();
        String[] columns = ParquetReader.getColumns();
        System.out.println(Arrays.asList(columns));
        HashMap<Integer, Integer> map = new HashMap<>();

        int[] nums = new int[]{-1};
        for (Row row : rowList) {
            for (String column : columns) {
                if (row.getAs(column) instanceof String) {
                    String str = row.getAs(column);
                }
                if (row.getAs(column) instanceof WrappedArray) {
                    WrappedArray<String> ary = row.getAs(column);
                }
            }
        }
    }

//    public static void main(String[] args) {
//        // create spark session for using spark
//        SparkSession spark = SparkSession.builder().master("local[1]").appName("Simple Application").getOrCreate();
//
//        /*
//        validate schema from here
//         */
//        // read parquet
//        String parquetPath = outputDataPath + "part-00000-edc4399a-8aa8-4d27-aed3-c9bef5339043-c000.snappy.parquet";
//        Dataset<Row> outputDataDF = spark.read().load(parquetPath); // default file format for spark is parquet, so use load() is okay
//        // read schema of parquet
//        List<String> outputFields = Arrays.asList(outputDataDF.schema().fieldNames());
//        System.out.println("111");
//        System.out.println(outputFields); // print all field names in this parquet
//
//        // read json format config file
//        String certainConfig = configPath + "study.config";
//        //read file into stream, try-with-resources
//        StringBuilder sb = new StringBuilder();
//        try (Stream<String> stream = Files.lines(Paths.get(certainConfig))) {
//
//            stream.forEach(sb::append);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // parse String into json obj
//        JsonObject jsonObject = new JsonParser().parse(sb.toString()).getAsJsonObject();
//        // there are two elements in config, first one is "input", second one is "output", take "output" for validate fields
//        JsonElement element = jsonObject.get("output");
//
//        // take out the String in "column" element
//        String outputSchemaString = element.getAsJsonArray().get(0).getAsJsonObject().get("column").getAsString();
//        List<String> inputFields = Arrays.asList(outputSchemaString.split(","));
//        System.out.println("222");
//        System.out.println(inputFields);
//
//        // compare number of fields first
//        boolean isSchemaCompatible = true;
//        // compare number of fields first
//        if (inputFields.size() == outputFields.size()) {
//            // compare one field with another
//            for (String s : inputFields) {
//                if (!outputFields.contains(s)) {
//                    isSchemaCompatible = false;
//                    break; // means one of input fields is not within the output fields, then schema incompatible
//                }
//            }
//        }
//
//        System.out.printf("Is schema compatible: %s. \n", isSchemaCompatible);
//
//        /*
//        validate first record of data from here
//         */
//        // read input data by using spark
//        Dataset<Row> inputDataDF = spark.read().format("csv")
//                .option("sep", "|")
//                .option("header", "true")
//                .load(inputDataPath + "12345_Study_201906111200_dv.csv");
//        String[] columnNames = inputDataDF.columns();
//
//        System.out.println("333");
//        boolean isDataCompatible = true;
//        for (String cname : columnNames) {
//            // todo: save dataset into List<String>
//            if (!inputDataDF.head().getAs(cname).equals(outputDataDF.head().getAs(cname.toUpperCase()))) {
//                isDataCompatible = false;
//                break;
//            }
//        }
//
//
//        System.out.printf("Is data compatible: %s. \n", isDataCompatible);
//        System.out.println();
//
//    }

    public static Boolean validateSchema(String inputPath, String outputPath) {
        return true;
    }

    public static Boolean validateData(String inputPath, String outputPath) {
        return true;
    }


}
