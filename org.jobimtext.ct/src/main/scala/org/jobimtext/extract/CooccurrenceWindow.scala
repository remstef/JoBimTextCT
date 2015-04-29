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

package org.jobimtext.extract

import org.apache.spark.rdd.RDD

/**
 * Created by Steffen Remus.
 */
object CooccurrenceWindow {

  def apply(windowsize:Int, lines_in:RDD[String]):RDD[String] = {

    def fun(id:Long, tokens:Seq[String], w:Int):TraversableOnce[(String, String, Long)] = if (w < 0 || tokens.length <= w) getCooccurrencesAll(id, tokens) else getCooccurrencesWindow(id, tokens, w)

    val lines_out = lines_in.map(line => (line.hashCode, line.split(' ')))
      .map({case (id, tokens) => fun(id, tokens, windowsize)})
      .flatMap(triples => triples.map(triple => "%s\t%s\t%s".format(triple._1, triple._2, triple._3)))
    return lines_out

  }

  def getCooccurrencesWindow(id:Long, tokens:Seq[String], windowsize:Int):TraversableOnce[(String, String, Long)] = {
    val windows = tokens.sliding(windowsize).map(_.toSeq)
    val windowsfinal = for (k <- 1 to windowsize-1) yield tokens.slice(tokens.length - k, tokens.length).toSeq
    val windowsall = windows++windowsfinal
    val triples = windowsall.flatMap(w => for (i <- 1 until w.length) yield Seq((w(0), w(i), id), (w(i),w(0),id)))
    return triples.flatMap(triples => triples)
  }


  def getCooccurrencesAll(id:Long, tokens:Seq[String]):TraversableOnce[(String, String, Long)] = {
    val triples = for(i <- 0 until tokens.length; j <- i+1 until tokens.length)
      yield Seq((tokens(i), tokens(j), id),(tokens(j),tokens(i),id))//  "%s\t%s\t%s\n%2$s\t%1$s\t%3$s".format(tokens(i), tokens(j), id)
    return triples.flatMap(triples => triples)
  }

}
