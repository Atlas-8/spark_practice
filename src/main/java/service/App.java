package service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;


@Slf4j
public class App {

    private static final String CONFIG_PATH = Objects.requireNonNull(Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("app.properties"))
                    .getPath();

    private static final String TARGET_PATH = Objects.requireNonNull(Thread.currentThread()
                    .getContextClassLoader()
                    .getResource(""))
                    .getPath() + "\\output";

    private static final Properties props = new Properties();


    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            throw new RuntimeException("file address expected");
        }

        try(FileInputStream fis = new FileInputStream(CONFIG_PATH)) {
            props.load(fis);
        }

        System.setProperty("hadoop.home.dir", props.getProperty("win_utils_path"));
        String partCol = props.getProperty("part_col");
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("field1", DataTypes.StringType, true),
                DataTypes.createStructField("field2", DataTypes.StringType, true),
                DataTypes.createStructField(partCol, DataTypes.TimestampType, true)
        });

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkTesting")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> inputTable = spark
                .read()
                .schema(schema)
                .format("csv")
                .option("partition_column", partCol)
                .load(args[0]);

        inputTable
                .write()
                .format("csv")
                .partitionBy(partCol)
                .mode(SaveMode.Overwrite)
                .save(TARGET_PATH);

        log.info("Writing complete. Output directory: {}", TARGET_PATH);
    }
}
