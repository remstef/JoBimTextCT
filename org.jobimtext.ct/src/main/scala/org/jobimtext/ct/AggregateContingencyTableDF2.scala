/*
 *   Copyright 2015
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.jobimtext.ct

/**
 * @author Steffen Remus
 *
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import breeze.linalg.{sum, DenseVector}
import org.apache.spark.rdd.RDD

object AggregateContingencyTableDF2 {
  
  def apply(lines_in:RDD[String]):RDD[String] = {
    
    val lines_out = lines_in.map(line => line.split('\t'))
      .map({case Array(docid, e1, e2, n11, n12, n21, n22) => ((e1,e2), DenseVector(n11.toInt, n12.toInt, n21.toInt, n22.toInt, 1))})
      .reduceByKey((a,b) => a + b)
      .map({case ((e1,e2), vec) => e1 + "\t" + e2 + vec.foldLeft("")((x,y) => x + "\t" + y.toString()) + "\t" + sum(vec(0 to -2)) })
    return lines_out
    
  }

  def classic(lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(line => line.split('\t'))
      .map({case Array(docid, e1, e2, n11, n12, n21, n22) => ((e1,e2), DenseVector(n11.toInt, n12.toInt, n21.toInt, n22.toInt, 1))})
      .reduceByKey((a,b) => a + b)
      .map({case ((e1,e2), vec) => "%s\t%s\t%s\t%s\t%s".format(e1, e2, vec(0), sum(vec(0,1)), sum(vec(0,2)))})
    return lines_out;

  }





  def main(args: Array[String]): Unit = {
//		if (args.size < 2) {
//			System.err.println("Usage: MainClass <in-file> <out-file>");
//			System.exit(1);
//		}

		val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[2]").set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
		val sc = new SparkContext(conf);

		val lines_in = sc.textFile("src/test/files/artifical-ct.txt");
    val lines_out = AggregateContingencyTableDF2(lines_in);
    //lines_out.saveAsTextFile("test.txt");
    lines_out.sortBy(x => x).collect().foreach(line => println(line));

		sc.stop();




	}

}