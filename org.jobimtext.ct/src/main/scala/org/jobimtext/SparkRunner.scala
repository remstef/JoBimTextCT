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

package org.jobimtext

import org.apache.spark.{SparkConf, SparkContext}
import org.jobimtext.classic.ClassicToCT
import org.jobimtext.ct2.{AggregateCT2, CT2Marginals, ProbsFromCT2}
import org.jobimtext.spark.SparkConfigured
import org.jobimtext.probabilistic.{KLDivergence, TopProbs}

/**
 * Created by Steffen Remus.
 */
object SparkRunner extends SparkConfigured{


  override def run(conf:SparkConf, args: Array[String]): Unit = {

    val topn = conf.getOption("topn").getOrElse({println("Setting 'topn' to '%d'.".format(300)); 300})

    val in = conf.getOption("in").getOrElse(throw new IllegalStateException("Missing input path. Specify with '-in=<file-or-dir>'."))
    val out = conf.getOption("out").getOrElse(throw new IllegalStateException("Missing output path. Specify with '-in=<file-or-dir>'."))

    val sc = new SparkContext(conf.setAppName("JoBimTextCT"));

    val lines_in = sc.textFile(in).filter(_.nonEmpty)

//        val lines_out = AggregateCT2(ClassicToCT(lines_in));
//        val lines_out = AggregateCT2.classic(ClassicToCT(lines_in));
//        val lines_out = AggregateCT(2, ClassicToCT(lines_in));
//        val lines_out = ClassicToCT(lines_in)
//        val lines_out = ClassicToCT.classicToAggregatedCT2(lines_in)

    val lines_out =
      KLDivergence(
        TopProbs(2,
          ProbsFromCT2(
            CT2Marginals(
              AggregateCT2.classic(
                ClassicToCT(lines_in)
              )
            )
          )
        )
      )

    lines_out.saveAsTextFile(out);

    sc.stop();
  }

  def main(args: Array[String]) {
    run(args)
  }


}
