package org.jobimtext.classic

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


/**
 * Created by stevo on 07/04/15.
 */
object CountCoocc {

  def apply(lines_in:RDD[String]):RDD[String] = {

    val coocc = lines_in.map(line => line.split("\t", 5))
      /* {case Array(docid, e1, e2, n11, rest) => ((e1, e2), n11.toInt)} */
      .map(arr => ((arr(1),arr(2)), arr(3).toInt))
      .reduceByKey((v1,v2) => v1 + v2)

    val e1occ = coocc.map({case ((e1, e2), n11) => (e1, n11)})
      .reduceByKey((v1,v2) => v1 + v2)

    val e2occ = coocc.map({case ((e1, e2), n11) => (e2, n11)})
      .reduceByKey((v1,v2) => v1 + v2)

    val joined = coocc.map({case ((e1, e2), n11) => (e1, (e2, n11))})
      .join(e1occ) /* (a,((c,4),7)) */
      .map({case (e1, ((e2,n11),n1dot)) => (e2, (e1, n11, n1dot))})
      .join(e2occ) /* (c,((a,4,7),9)) */
      .map({case (e2, ((e1,n11,n1dot), ndot1)) => (e1, e2, n11, n1dot, ndot1)})

    val lines_out = joined.map({case (e1, e2, n11, n1dot, n2dot) => "%s\t%s\t%s\t%s\t%s".format(e1,e2,n11,n1dot,n2dot)})

    return lines_out;
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[2]").set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
    val sc = new SparkContext(conf);

    val lines_in = sc.textFile("src/test/files/artifical-ct.txt");
    val lines_out = CountCoocc(lines_in);
    //lines_out.saveAsTextFile("test.txt");
    lines_out.sortBy(x => x).collect().foreach(line => println(line));

    sc.stop();
  }

}
