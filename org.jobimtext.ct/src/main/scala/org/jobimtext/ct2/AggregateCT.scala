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
import org.jobimtext.misc.Ctconf

object AggregateCT {

  /**
   *
   * @param lines_in (docid,u1,u2,n11,n12,n21,n22)
   * @return (u1,u2,n11,n12,n21,n22,ndocs)
   */
  def apply(ctconf: Ctconf = Ctconf.noFilter, lines_in:RDD[String]):RDD[String] = {
    return perdoc(ctconf, lines_in)
  }

  /**
   *
   * CT per document
   *
   * @param lines_in (docid,u1,u2,n11,n12,n21,n22,o12,o21,o22)
   * @return (u1,u2,n11,n12,n21,n22,o12,o21,o22,ndocs)
   */
  def perdoc(ctconf: Ctconf = Ctconf.noFilter, lines_in:RDD[String]):RDD[String] = {

    val lines_out = lines_in.map(line => line.split('\t'))
      .map(arr => CT2.fromStringArray(arr.takeRight(arr.length -1)))
      .map(ct => ((ct.u1, ct.u2), ct))
      .reduceByKey((r,c) => (r + c))
      .filter({case ((u1,u2), ct2) => ctconf.filterCT(ct2)})
      .map({case ((u1,u2), ct) => ct.toString()})
    return lines_out

  }

  /**
   *
   * classic, i.e. globally, corpus wide counts
   *
   * TODO: think! could this can be done more efficiently?
   *
   * @param lines_in (docid,u1,u2,n11,n12,n21,n22)
   * @return (u1,u2,n11,n12,n21,n22,1)
   */
  def classic(ctconf: Ctconf = Ctconf.noFilter, lines_in:RDD[String]):RDD[String] = {

    // forget about n12,n21 and so on, since we only need n11
    val coocc = lines_in.map(line => line.split("\t", 5))
      .map({case Array(docid,u1, u2, n11,rest) => ((u1, u2), (n11.toFloat, 1))})
      .reduceByKey((r,c) => (r._1 + c._1, r._2 + c._2)) // (u1,u2),(n11,ndocs)
    coocc.cache()

    val u1occ = coocc.map({case ((u1, u2), (n11, ndocs)) => (u1, (n11,1f))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2)) // (u1),(n1dot,o1dot)
      .filter({case ((u1),(n1dot,o1dot)) => n1dot >= ctconf.min_n1dot})

    val u2occ = coocc.map({case ((u1, u2), (n11, ndocs)) => (u2, (n11,1f))})
      .reduceByKey((r,c) => (r._1+c._1,r._2+c._2)) //(u2),(ndot1,odot1)
      .filter({case ((u2),(ndot1,odot1)) => ndot1 >= ctconf.min_ndot1 && odot1 >= ctconf.min_odot1 && odot1 <= ctconf.max_odot1})

    val joined = coocc.filter({case ((u1,u2),(n11,ndocs)) => n11 >= ctconf.min_n11 && ndocs >= ctconf.min_docs})
      .map({case ((u1, u2), (n11, ndocs)) => (u1, (u2, ndocs, n11))})
      .join(u1occ) /* (a,((c,ndocs,n11),(n1dot,o1dot))) */
      .map({case (u1, ((u2,ndocs,n11),(n1dot,o1dot))) => (u2, (u1, ndocs, n11, (n1dot,o1dot)))})
      .join(u2occ) /* (c,((a,ndocs, n11,(n1dot,o1dot)),(ndot1,odot1))) */
      .map({case (u2, ((u1,ndocs,n11,(n1dot,o1dot)), (ndot1,odot1))) => (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1) )})

    coocc.unpersist()

    val n_ = joined.map({case (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1)) => n11}).sum()
    var n = 1f
    if(n_ > Float.MaxValue)
      n = Float.MaxValue
    else
      n = n_.toFloat
    val o = joined.count().toFloat

    val lines_out = joined.map({ case (u1, u2, ndocs, n11, (n1dot,o1dot), (ndot1,odot1) ) => new CT2(u1, u2, n11, n1dot-n11, ndot1-n11, n-n1dot-ndot1+n11, o1dot-1f, odot1-1f, o-o1dot-odot1+1f, ndocs).toString()})

    return lines_out;

  }

}