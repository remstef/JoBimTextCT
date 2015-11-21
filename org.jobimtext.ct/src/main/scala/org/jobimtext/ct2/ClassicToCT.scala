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

package org.jobimtext.ct2

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by Steffen Remus.
 */
object ClassicToCT {

  def apply(lines_in:RDD[String]):RDD[String] = {

    val ct_per_doc = lines_in.map(line => line.split("\t", 4))
      /* {case Array(u1, u2, docid, rest) => ((docid, u1, u2), 1)} */
      .map(arr => ((arr(2),arr(0),arr(1)), 1f))
      .reduceByKey((v1,v2) => v1 + v2)
      .map({case ((docid, u1, u2), n11) => (docid, (u1,u2,n11))})
      .groupByKey()
      .map({case (docid, counts_per_doc) => ctFromDoc(docid, counts_per_doc)})
      .flatMap(l => l)

    // (docid,u1,u2,n11,n12,n21,n22,n12_,n21_,n22_)
    val lines_out = ct_per_doc.map({case (docid,ct2) => docid + "\t" + ct2.toString()})

    return lines_out

  }

  def ctFromDoc(docid:String, counts_per_doc:Iterable[(String,String,Float)]):Iterable[(String, CT2[String])] = {

    val n = counts_per_doc.map(_._3).sum
    val o = counts_per_doc.size

    // u1, freq, occurrences
    val n1dot_counts = counts_per_doc.map({case (u1,u2,n11) => (u1, n11, 1f)})
      .groupBy({case (u1, n11, o11) => u1 })
      .map({case (u1, iter_u1_n11_o11) => (u1, iter_u1_n11_o11.reduceLeft((r,c) => (r._1, r._2+c._2, r._3+c._3))) }) /* eq. reduceByKey */

    // u2, freq, occurrences
    val ndot1_counts = counts_per_doc.map({case (u1,u2,n11) => (u2, n11, 1f)})
      .groupBy({case (u2, n11, o11) => u2 })
      .map({case (u2, iter_u2_n11_o11) => (u2, iter_u2_n11_o11.reduceLeft((r,c) => (r._1, r._2+c._2, r._3+c._3))) }) /* eq. reduceByKey */

    def get_summed_value(u:String):(Float,Float) = {
      val x = ndot1_counts.get(u) match {
        case Some(x) => (x._2, x._3)
        case None => (0f,0f)
      }
      val y = n1dot_counts.get(u) match {
        case Some(y) => (y._2, y._3)
        case None => (0f,0f)
      }
      return (x._1+y._1, x._2+y._2)
    }

    val joined = counts_per_doc.map({case (u1, u2, n11) => (u1, u2, n11, get_summed_value(u1), get_summed_value(u2), n, o)})
      .map({case (u1,u2,n11,(n1dot,o1dot),(ndot1,odot1),n,o) => (docid, new CT2[String](u1,u2,n11,n1dot-n11,ndot1-n11,n-n1dot-ndot1+n11, o1dot-1f, odot1-1f, o-o1dot-odot1+1f, 1))})

    return joined

  }

  /**
   *
   * @param lines_in input format: 'jo <tab> bim <tab> count <tab-or-no-tab> ...
   * @return ct2format
   */
  def classicWordFeatureCountToAggregatedCT2(lines_in:RDD[String]):RDD[String] = {

    val coocc = lines_in.map(line => line.split("\t", 3))
      .map({case Array(u1, u2, n11) => ((u1, u2), n11.toFloat)})
      .reduceByKey((v1,v2) => v1 + v2)
    coocc.cache()

    val u1occ = coocc.map({case ((u1, u2), n11) => (u1, (n11,1f))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2))

    val u2occ = coocc.map({case ((u1, u2), n11) => (u2, (n11,1f))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2))

    val joined = coocc.map({case ((u1, u2), n11) => (u1, (u2, n11))})
      .join(u1occ) /* (a,((c,n11),(n1dot,o1dot))) */
      .map({case (u1, ((u2,n11),(n1dot,o1dot))) => (u2, (u1, n11, (n1dot,o1dot)))})
      .join(u2occ) /* (c,((a,n11,(n1dot,o1dot)),(ndot1,odot1))) */
      .map({case (u2, ((u1,n11,(n1dot,o1dot)), (ndot1,odot1))) => (u1, u2, n11, (n1dot,o1dot), (ndot1,odot1) )})

    coocc.unpersist()

    val n_ = joined.map({case (u1, u2, n11, n1dot, ndot1) => n11}).sum()
    var n = 1f
    if(n_ > Float.MaxValue)
      n = Float.MaxValue
    else
      n = n_.toFloat
    val o = joined.count().toFloat

    val lines_out = joined.map({ case (u1, u2, n11, (n1dot,o1dot), (ndot1,odot1) ) => new CT2[String](u1, u2, n11, n1dot-n11, ndot1-n11, n-n1dot-ndot1+n11, o1dot-1f, odot1-1f, o-o1dot-odot1+1f, 1).toString()})

    return lines_out;
  }

  def classicToAggregatedCT2(lines_in:RDD[String]):RDD[String] = {

    val word_feature_count = lines_in.map(line => line.split("\t", 3))
      .map(arr => ((arr(0),arr(1)), 1d))
      .map({case ((w,f), c) => "%s\t%s\t%.0f".format(w,f,c)})

    val lines_out = classicWordFeatureCountToAggregatedCT2(word_feature_count)

    return lines_out;
  }

}
