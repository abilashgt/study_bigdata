package examples

/**
  * Created by Abilash George Thomas on 9/6/2016.
  *
  * Reference:
  * https://www.cloudera.com/documentation/enterprise/5-5-x/topics/spark_develop_run.html
  */
import conf.AHiveContext

object HiveExample {
  def main(args: Array[String]) {
    val hc = AHiveContext.getHiveContext()
    val df = hc.sql("select * from study_hive.test_table")
    df.show
  }
}