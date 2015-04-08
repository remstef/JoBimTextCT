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
 * Created by Steffen Remus.
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

    val coocc = lines_in.map(line => line.split("\t", 5))
      /* {case Array(docid, e1, e2, n11, rest) => ((e1, e2), n11.toInt)} */
      .map(arr => ((arr(1), arr(2)), arr(3).toInt))
      .reduceByKey((v1, v2) => v1 + v2)

    val e1occ = coocc.map({ case ((e1, e2), n11) => (e1, n11) })
      .reduceByKey((v1, v2) => v1 + v2)

    val e2occ = coocc.map({ case ((e1, e2), n11) => (e2, n11) })
      .reduceByKey((v1, v2) => v1 + v2)

    val joined = coocc.map({ case ((e1, e2), n11) => (e1, (e2, n11)) })
      .join(e1occ) /* (a,((c,4),7)) */
      .map({ case (e1, ((e2, n11), n1dot)) => (e2, (e1, n11, n1dot)) })
      .join(e2occ) /* (c,((a,4,7),9)) */
      .map({ case (e2, ((e1, n11, n1dot), ndot1)) => (e1, e2, n11, n1dot, ndot1) })

    val n = joined.map({ case (e1, e2, n11, n1dot, ndot1) => n11 }).sum().toLong;

    val lines_out = joined.map({ case (e1, e2, n11, n1dot, ndot1) => "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d".format(e1, e2, n11, (n1dot-n11), (ndot1-n11), n - (n1dot + ndot1) + n11, 1, n) })

    return lines_out
  }

}