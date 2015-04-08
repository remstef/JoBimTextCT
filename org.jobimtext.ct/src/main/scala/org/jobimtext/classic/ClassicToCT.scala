/*
 *
 *  Copyright 2015.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.jobimtext.classic

import breeze.linalg.{sum, DenseVector}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by Steffen Remus.
 */
object ClassicToCT {

  def apply(lines_in:RDD[String]):RDD[String] = {

    val ct_per_doc = lines_in.map(line => line.split("\t", 4))
      /* {case Array(e1, e2, docid, rest) => ((docid, e1, e2), 1)} */
      .map(arr => ((arr(2),arr(0),arr(1)), 1))
      .reduceByKey((v1,v2) => v1 + v2)
      .map({case ((docid, e1, e2), n11) => (docid, (e1,e2,n11))})
      .groupByKey()
      .map({case (docid, counts_per_doc) => ctFromDoc(docid, counts_per_doc)})
      .flatMap(l => l)

    val ct_aggregated = ct_per_doc.map({case (docid, e1,e2,n11,n12,n21,n22) => ((e1,e2), DenseVector(n11,n12,n21,n22,1))})
      .reduceByKey(_+_)

    val lines_out = ct_aggregated.map({case ((e1,e2), vec) => e1 + "\t" + e2 + vec.foldLeft("")((x,y) => x + "\t" + y.toString()) + "\t" + sum(vec(0 to -2)) })

    return lines_out

  }

  def ctFromDoc(docid:String, counts_per_doc:Iterable[(String,String,Int)]):Iterable[(String, String,String,Int,Int,Int,Int)] = {

    val n = counts_per_doc.map(_._3).sum

    val n1dot_counts = counts_per_doc.map({case (e1,e2,n11) => (e1, n11)})
      .groupBy({case (e1, n11) => e1 }).map({case (e1, iter_e1_n11) => (e1, iter_e1_n11.map(_._2).sum)}) /* eq. reduceByKey */

    val ndot1_counts = counts_per_doc.map({case (e1,e2,n11) => (e2, n11)})
      .groupBy({case (e2, n11) => e2 }).map({case (e2, iter_e2_n11) => (e2, iter_e2_n11.map(_._2).sum)}) /* eq. reduceByKey */

    def get_summed_value(e:String):Int = {
      val x = ndot1_counts.get(e) match {
        case Some(x) => x
        case None => 0
      }
      val y = n1dot_counts.get(e) match {
        case Some(y) => y
        case None => 0
      }
      return x+y
    }

    val joined = counts_per_doc.map({case (e1, e2, n11) => (e1, e2, n11, get_summed_value(e1), get_summed_value(e2), n)})
      .map({case (e1,e2,n11,n1dot,ndot1,n) => (docid, e1, e2, n11, n1dot-n11, ndot1-n11, n-n1dot-ndot1+n11 )})

    return joined

  }

  def classic(lines_in:RDD[String]):RDD[String] = {

    val coocc = lines_in.map(line => line.split("\t", 3))
      /* {case Array(e1, e2, rest) => ((e1, e2), n11.toInt)} */
      .map(arr => ((arr(0),arr(1)), 1))
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

    val n = joined.map({case (e1, e2, n11, n1dot, ndot1) => n11}).sum().toLong;

    val lines_out = joined.map({ case (e1, e2, n11, n1dot, ndot1) => "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d".format(e1, e2, n11, (n1dot-n11), (ndot1-n11), n - (n1dot + ndot1) + n11, 1, n) })

    return lines_out;
  }

}
