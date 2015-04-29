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

import java.util

import org.apache.spark.rdd.RDD
import scala.math.{min,max}
import java.lang.Long._
import scala.reflect.internal.util.Collections


/**
 * Created by Steffen Remus.
 */
object NgramWithHole {

  def apply(n:Int = 3, allcombinations:Boolean = false, lines_in:RDD[String]):RDD[String] = {

    def fun(id:Long, tokens:Seq[String], n:Int=3):TraversableOnce[(String, String, Long)] = if (allcombinations) getAllCombinations(id, tokens, n) else getCenterCombinations(id,tokens,n)

    val lines_out = lines_in.map(line => (line.hashCode, line.split(' ')))
      .filter(_._2.length >= n)
      .map({case (id, tokens) => fun(id, tokens, n)})
      .flatMap(triples => triples.map(triple => "%s\t%s\t%s".format(triple._1, triple._2, toHexString(triple._3))))
    return lines_out

  }

  def getCenterCombinations(id:Long, tokens:Seq[String], n:Int=3):TraversableOnce[(String, String, Long)] = {
    val ngrams = tokens.sliding(n).map(_.toSeq)
    val m = n/2

    val result_ngram_limits_begin = for (i <- 0 until min(m, tokens.length))
      yield (tokens(i), "%s @ %s".format(tokens.slice(0, i).mkString(" "), tokens.slice(i+1,n).mkString(" ")).trim, id)

    val result_ngram_limits_end = for (i <- max(tokens.length-m,0) until tokens.length)
      yield (tokens(i), "%s @ %s".format(tokens.slice(tokens.length-n, i).mkString(" "), tokens.slice(i+1, tokens.length).mkString(" ")).trim, id)

    val result_ngram_center = ngrams.map(ngram => (ngram(m), "%s @ %s".format(ngram.take(m).mkString(" ").trim, ngram.takeRight(n-1-m).mkString(" ")).trim, id))

    return result_ngram_limits_begin++result_ngram_center++result_ngram_limits_end

  }


  def getAllCombinations(id:Long, tokens:Seq[String], n:Int=3):TraversableOnce[(String, String, Long)] = {
    val ngrams = tokens.sliding(n).map(_.toSeq)
    ngrams.flatMap(ngram =>
      for (i <- 0 until ngram.length)
        yield (ngram(i), "%s @ %s".format(ngram.take(i).mkString(" ").trim, ngram.takeRight(n-1-i).mkString(" ")).trim(), id)
    )
  }


  def main(args: Array[String]) {
    NgramWithHole.getAllCombinations(1l,"a b c".split(' '),3).foreach(println(_))
    println("---")
    NgramWithHole.getAllCombinations(1l,"a b c d e".split(' '),3).foreach(println(_))
    println("---")
    NgramWithHole.getCenterCombinations(1l,"a b c".split(' '),3).foreach(println(_))
    println("---")
    NgramWithHole.getCenterCombinations(1l,"a b c d e".split(' '),3).foreach(println(_))
  }
}
