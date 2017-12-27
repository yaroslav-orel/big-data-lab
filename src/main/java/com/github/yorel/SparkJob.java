package com.github.yorel;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

@UtilityClass
public class SparkJob {

    public void main(String[] args){
        val sparkContext = initSparkContext();
        val sparkSession = initSparkSession(sparkContext);

        val rdd = loadRDDFromCassandra(sparkContext);
        val dataFrame = toDataFrame(sparkSession, rdd);

        val result = getMostPopularCountries(sparkSession, dataFrame);
        result.show();
    }

    private Dataset<Row> getMostPopularCountries(SparkSession sparkSession, Dataset<Row> dataframe) {
        val query = "SELECT countrycode AS country, count(sourceip) as visits " +
                    "FROM uservisit " +
                    "GROUP BY countrycode " +
                    "ORDER BY visits DESC";

        dataframe.createOrReplaceTempView("uservisit");
        return sparkSession.sql(query).limit(10);
    }

    private Dataset<Row> toDataFrame(SparkSession sparkSession, CassandraTableScanJavaRDD<Uservisit> rdd) {
        return sparkSession.createDataFrame(rdd.rdd(), Uservisit.class);
    }

    private CassandraTableScanJavaRDD<Uservisit> loadRDDFromCassandra(SparkContext sparkContext) {
        return CassandraJavaUtil.javaFunctions(sparkContext).cassandraTable("lab", "uservisit", mapRowTo(Uservisit.class));
    }

    private SparkContext initSparkContext(){
        val conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .setAppName("Reading data from Cassanrda")
                .setMaster("spark://yorel-VirtualBox:7077");
        return new SparkContext(conf);
    }

    private SparkSession initSparkSession(SparkContext context){
        return SparkSession.builder().sparkContext(context).getOrCreate();
    }

}
