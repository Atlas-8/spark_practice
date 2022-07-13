package service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class AppTest {

    private static final String TARGET_DIR = "C:\\Users\\Professional\\Desktop\\Data\\Works\\spark_practice\\src\\main\\resources\\output\\*";
    private static final String INPUT_DIR = "C:\\Users\\Professional\\Desktop\\Data\\Works\\spark_practice\\src\\test\\resources\\test_input.csv";

    @BeforeAll
    public static void setUp(){
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
    }

    @Test
    public void mainMethodTest() throws IOException {

        String[] args = {"src/test/resources/test_input.csv"};
        App.main(args);

        Dataset<Row> written_df = SPARK_LOCAL
                .read()
                .csv(TARGET_DIR);
        assertEquals(4, written_df.count());

        Dataset<Row> control_df = SPARK_LOCAL
                .read()
                .schema(schema)
                .csv(INPUT_DIR)
                .drop("part_date");

        assertTrue(Arrays.deepEquals(
                (Row[]) control_df.collect(),
                (Row[]) written_df.collect()
        ));
    }

    @Test
    public void launchWithoutArgs(){
        String[] noArgs = {};
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            App.main(noArgs);
        });
        assertEquals("file address expected", thrown.getMessage());
    }

    private final SparkSession SPARK_LOCAL = SparkSession
            .builder()
            .appName("SparkTesting")
            .master("local[2]")
            .getOrCreate();

    private final StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("field1", DataTypes.StringType, true),
            DataTypes.createStructField("field2", DataTypes.StringType, true),
            DataTypes.createStructField("part_date", DataTypes.TimestampType, true)
    });

}
