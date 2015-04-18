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
package org.jobimtext.ct2

/**
 * Created by Steffen Remus.
 */

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object AggregateCT {

  /**
   *
   * @param lines_in (docid,e1,e2,n11,n12,n21,n22)
   * @return (e1,e2,n11,n12,n21,n22,ndocs)
   */
  def apply(lines_in:RDD[String]):RDD[String] = {
    return perdoc(lines_in)
  }

  /**
   *
   * CT per document
   *
   * @param lines_in (docid,e1,e2,n11,n12,n21,n22,o12,o21,o22)
   * @return (e1,e2,n11,n12,n21,n22,o12,o21,o22,ndocs)
   */
  def perdoc(lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(line => line.split('\t'))
      .map(arr => CT2.fromStringArray(arr.takeRight(arr.length -1)))
      .map(ct => ((ct.u1, ct.u2), ct))
      .reduceByKey((r,c) => (r + c))
      .map({case ((e1,e2), ct) => ct.toString("%.0f")})
    return lines_out

  }

  /**
   *
   * classic, i.e. globally, corpus wide counts
   *
   * TODO: could this can be done more efficiently?
   *
   * @param lines_in (docid,e1,e2,n11,n12,n21,n22)
   * @return (e1,e2,n11,n12,n21,n22,1)
   */
  def classic(lines_in:RDD[String]):RDD[String] = {

//    val coocc = lines_in.map(line => line.split("\t", 5))
//      /* {case Array(docid, e1, e2, n11, rest) => ((e1, e2), n11.toDouble)} */
//      .map(arr => ((arr(1), arr(2)), arr(3).toDouble))
//      .reduceByKey((v1, v2) => v1 + v2)
//
//    val e1occ = coocc.map({ case ((e1, e2), n11) => (e1, n11) })
//      .reduceByKey((v1, v2) => v1 + v2)
//
//    val e2occ = coocc.map({ case ((e1, e2), n11) => (e2, n11) })
//      .reduceByKey((v1, v2) => v1 + v2)
//
//    val joined = coocc.map({ case ((e1, e2), n11) => (e1, (e2, n11)) })
//      .join(e1occ) /* (a,((c,4),7)) */
//      .map({ case (e1, ((e2, n11), n1dot)) => (e2, (e1, n11, n1dot)) })
//      .join(e2occ) /* (c,((a,4,7),9)) */
//      .map({ case (e2, ((e1, n11, n1dot), ndot1)) => (e1, e2, n11, n1dot, ndot1) })
//
//    val n = joined.map({ case (e1, e2, n11, n1dot, ndot1) => n11 }).sum().toLong;
//
//    val lines_out = joined.map({ case (e1, e2, n11, n1dot, ndot1) => "%s\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t1".format(e1, e2, n11, (n1dot-n11), (ndot1-n11), n - (n1dot + ndot1) + n11) })

    // copied from ClassicToCT.classicWordFeatureCountToAggregatedCT2
    // forget about n12,n21 and so on, since we only need n11
    val coocc = lines_in.map(line => line.split("\t", 5))
      .map({case Array(docid,u1, u2, n11,rest) => ((u1, u2), (n11.toDouble, 1l))})
      .reduceByKey((r,c) => (r._1 + c._1, r._2 + c._2)) // (u1,u2),(n11,ndocs)

    val u1occ = coocc.map({case ((u1, u2), (n11, ndocs)) => (u1, (n11,1d))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2))

    val u2occ = coocc.map({case ((u1, u2), (n11, ndocs)) => (u2, (n11,1d))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2))

    val joined = coocc.map({case ((u1, u2), (n11, ndocs)) => (u1, (u2, ndocs, n11))})
      .join(u1occ) /* (a,((c,ndocs,n11),(n1dot,o1dot))) */
      .map({case (u1, ((u2,ndocs,n11),(n1dot,o1dot))) => (u2, (u1, ndocs, n11, (n1dot,o1dot)))})
      .join(u2occ) /* (c,((a,ndocs, n11,(n1dot,o1dot)),(ndot1,odot1))) */
      .map({case (u2, ((u1,ndocs,n11,(n1dot,o1dot)), (ndot1,odot1))) => (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1) )})

    val n = joined.map({case (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1)) => n11}).sum()
    val o = joined.count().toDouble

    val lines_out = joined.map({ case (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1) ) => new CT2(u1, u2, n11, n1dot-n11, ndot1-n11, n-n1dot-ndot1+n11, o1dot-1d, odot1-1d, o-o1dot-odot1+1d, ndocs).toString("%.0f")})

    return lines_out;

  }

}