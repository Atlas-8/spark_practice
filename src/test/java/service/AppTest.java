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

import static org.junit.jupiter.api.Assertions.*;


public class AppTest {

    private static final String ABSOLUTE_PATH = App.ABSOLUTE_PATH;
    private static final String TARGET_DIR = System.getProperty("user.dir") + "\\output";
    private static final String INPUT_PATH = ABSOLUTE_PATH + "\\src\\test\\resources\\test_data.csv";

    @BeforeAll
    public static void setUp(){
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
    }

    @Test
    public void mainMethodTest() throws IOException {

        String[] args = {INPUT_PATH};
        App.main(args);

        Dataset<Row> written_df = TEST_SESSION
                .read()
                .csv(TARGET_DIR);

        Dataset<Row> control_df = TEST_SESSION
                .read()
                .schema(schema)
                .csv(INPUT_PATH);

        written_df.show();
        control_df.show();

        assertEquals(control_df.count(), written_df.count());
        assertTrue(written_df.collectAsList().containsAll(control_df.collectAsList()));
        assertTrue(control_df.collectAsList().containsAll(written_df.collectAsList()));

    }

    @Test
    public void launchWithoutArgs(){
        String[] noArgs = {};
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            App.main(noArgs);
        });
        assertEquals("file address expected", thrown.getMessage());
    }

    private final SparkSession TEST_SESSION = SparkSession
            .builder()
            .appName("SparkPracticeTest")
            .master("local[1]")
            .getOrCreate();

    private final StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("field1", DataTypes.StringType, true),
            DataTypes.createStructField("field2", DataTypes.StringType, true),
            DataTypes.createStructField("part_date", DataTypes.TimestampType, true)
    });

}
