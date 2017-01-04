package conf

import org.apache.spark.sql.SQLContext

/**
  * Created by 984620 on 9/22/2016.
  */
object ASqlContext {
    val sqlContext = new SQLContext(ASparkContext.getSparkContext())

    def getSqlContest(): SQLContext = {
        return sqlContext;
    }

}
