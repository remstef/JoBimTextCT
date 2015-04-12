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

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jobimtext.classic.ClassicToCT
import org.jobimtext.ct2.{SumMarginalsCT, ProbsFromCT}
import org.jobimtext.misc.SimSortTopN
import org.jobimtext.probabilistic._
import org.jobimtext.spark.SparkConfigured

/**
  * Created by Steffen Remus.
  */
object ShellRunner {

  /**
   *
   * execute this snippet from your spark shell:
   *
   *  import org.jobimtext.run.ShellRunner
   *  ShellRunner.kl(...)
   *
   * @param sc
   * @param in
   * @param out
   * @param sort_output
   * @param reverse_sorting
   * @param trimtopn
   * @return
   */
  def kl(sc:SparkContext,
         in:String,
         out:String,
         sort_output:Boolean = false,
         reverse_sorting:Boolean = false,
         trimtopn:Int = 20
          ):RDD[String] = {

   val joinedprobs = sc.textFile(in).filter(_.nonEmpty)
   var kl = KLDivergenceRdcBy(joinedprobs)
   if(sort_output)
     kl = SimSortTopN(topn = trimtopn, reverse = reverse_sorting, kl)
   kl.saveAsTextFile(out)

   return kl

  }

 }
