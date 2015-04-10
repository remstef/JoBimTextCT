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

package org.jobimtext.run

import java.io.FileNotFoundException

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jobimtext.classic.ClassicToCT
import org.jobimtext.ct2.{CT2Marginals, ProbsFromCT2}
import org.jobimtext.misc.SimSortTopN
import org.jobimtext.probabilistic.{KLDivergence, TopProbs}
import org.jobimtext.spark.SparkConfigured

import scala.util.Try

/**
 * Created by Steffen Remus.
 */
object KLRunner1 extends SparkConfigured{

  def main(args: Array[String]):Unit = {
    run(args)
  }

  override def run(conf:SparkConf, args: Array[String]): Unit = {

    val topn = conf.getOption("topn").getOrElse({println("Setting 'topn' to '%d'.".format(300)); 300}).asInstanceOf[Int]
    val sort_out = conf.getOption("sort").getOrElse({println("Sort output: '%s'.".format(false)); false}).asInstanceOf[Boolean]

    val in = conf.getOption("in").getOrElse(throw new IllegalStateException("Missing input path. Specify with '-in=<file-or-dir>'."))
    val out = conf.getOption("out").getOrElse(throw new IllegalStateException("Missing output path. Specify with '-out=<dir>'."))

    val sc = new SparkContext(conf.setAppName("JoBimTextCT"))

    val kl = run(sc,in,out,topn,sort_out)

    sc.stop()

  }

  def run(sc:SparkContext,
          in:String,
          out:String,
          topn:Int = 300,
          sort_output:Boolean = false,
          reverse_sorting:Boolean = false,
          trimtopn:Int = 20
           ):RDD[String] = {

    val lines_in = sc.textFile(in).filter(_.nonEmpty)
    val cts = ClassicToCT.classicWordFeatureCountToAggregatedCT2(lines_in)
    cts.saveAsTextFile(out + "_ct")

    val probs = TopProbs(topn, ProbsFromCT2(CT2Marginals(cts)))
    probs.saveAsTextFile(out + "_p")

    var kl = KLDivergence(probs)
    if(sort_output)
      kl = SimSortTopN(kl,topn = trimtopn, reverse = reverse_sorting)
    kl.saveAsTextFile(out + "_kl")

//    var cts:RDD[String] = null
//    try {
//      cts = sc.textFile(out + "_ct").filter(_.nonEmpty)
//    } catch {
//      case e => {
//        e.printStackTrace()
//        val lines_in = sc.textFile(in).filter(_.nonEmpty)
//        cts = ClassicToCT.classicWordFeatureCountToAggregatedCT2(lines_in)
//        cts.saveAsTextFile(out + "_ct");
//      }
//    }

//    var probs:RDD[String] = null
//    try {
//      probs = sc.textFile(out + "_p").filter(_.nonEmpty)
//    } catch {
//      case e => {
//        e.printStackTrace()
//        val probs = TopProbs(topn, ProbsFromCT2(CT2Marginals(cts)))
//        probs.saveAsTextFile(out + "_p");
//      }
//    }

//    var kl:RDD[String] = null
//    try {
//      kl = sc.textFile(out + "_kl").filter(_.nonEmpty)
//    } catch {
//      case e => {
//        e.printStackTrace()
//        var kl = KLDivergence(probs)
//        if(sort_output)
//          kl = SimSortTopN(kl,topn = trimtopn, reverse = reverse_sorting)
//        kl.saveAsTextFile(out + "_kl");
//      }
//    }

    return kl
  }

}
