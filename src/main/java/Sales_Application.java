import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Sales_Application {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("Region", DataTypes.StringType)
                .add("Country", DataTypes.StringType)
                .add("ItemType", DataTypes.StringType)
                .add("SalesChannel", DataTypes.StringType)
                .add("OrderPriority", DataTypes.StringType)
                .add("OrderDate", DataTypes.StringType)
                .add("OrderID", DataTypes.IntegerType)
                .add("ShipDate", DataTypes.StringType)
                .add("UnitsSold", DataTypes.StringType)
                .add("UnitPrice", DataTypes.StringType)
                .add("UnitCost", DataTypes.StringType)
                .add("TotalRevenue", DataTypes.StringType)
                .add("TotalCost", DataTypes.StringType)
                .add("TotalProfit", DataTypes.StringType);


        SparkSession sparkSession = SparkSession
                .builder().master("local")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/product.sales")
                .appName("Sales").getOrCreate();


        Dataset<Row> rawData = sparkSession.read().option("header",true).schema(schema).csv("C:\\sales.csv");
        //rawData.show();

        Dataset<Row> data= rawData.filter(rawData.col("OrderID").isNotNull());
        //data.show();
        System.out.println(data.count());
        //Ürünlerin bölgelere göre online/offline satıs durumu
        Dataset<Row> resultDS = data.groupBy("Region","ItemType","SalesChannel").count().sort(functions.desc("count"));
        resultDS.show();
        MongoSpark.write(resultDS).mode("overwrite").save();


    }
}