package com.github.yorel;

import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

@UtilityClass
public class SparkJob {

    public void main(String[] args){
        val sparkSession = initSparkSession();
        val dataFrame = getDataFrame(sparkSession);
        val result = getMostPopularCountries(sparkSession, dataFrame);
        result.show();
    }

    private Dataset<Row> getMostPopularCountries(SparkSession sparkSession, Dataset<Row> dataframe) {
        val query = "SELECT countrycode AS country, count(sourceip) AS visits " +
                    "FROM uservisit " +
                    "GROUP BY countrycode " +
                    "ORDER BY visits DESC";

        dataframe.createOrReplaceTempView("uservisit");
        return sparkSession.sql(query).limit(10);
    }

    private Dataset<Row> getDataFrame(SparkSession sparkSession) {
        val rdd = javaFunctions(sparkSession.sparkContext())
                .cassandraTable("lab", "uservisit", mapRowTo(Uservisit.class));
        return sparkSession.createDataFrame(rdd.rdd(), Uservisit.class);
    }

    private SparkSession initSparkSession(){
        val conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .setAppName("Reading data from Cassanrda")
                .setMaster("spark://yorel-VirtualBox:7077");
        return SparkSession.builder().sparkContext(new SparkContext(conf)).getOrCreate();
    }

}
