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

import de.tudarmstadt.lt.scalautils.FixedSizeTreeSet
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jobimtext._
import org.jobimtext.ct2.{CT2, AggregateCT, ClassicToCT}
import org.jobimtext.extract.{NgramWithHole, CooccurrenceWindow}
import org.jobimtext.misc._
import org.jobimtext.sim._

/**
 * Created by Steffen Remus.
 */
object TestRunnerSpark {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("App")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[FixedSizeTreeSet[_]], classOf[CT2[_]]))


    val sc = new SparkContext(conf);

    val lines_in = sc.textFile("/user/hadoop/rem/simplewikipedia/in").filter(_.nonEmpty).repartition(sc.defaultParallelism)

    val ctconf = Ctconf(
      min_ndot1 = 2, // min occurrences jo
      min_n1dot = 2, // min occurrences bim
      min_n11 = 2, // min occurrences jo-bim
      max_odot1 = 1000000000, // max different occurrences jo
      min_odot1 = 2, // min different occurrences jo
      min_docs = 2, // min docs
      min_sig = Double.NegativeInfinity,
      topn_f = 1000, // take top n contexts per jo
      topn_s = 100, // take top n similar jos per jo
      min_sim = 2
    )

    val lines_out =
      TakeTopN(100,true,true,
        FreqSim(with_features = false,
          JoinBySharedFeaturesGrpBy(-1,
            ct2.sig.FreqFromCT(
              ct2.AggregateCT.classic(ctconf,
                ClassicToCT(
                  NgramWithHole(3,false,lines_in)
                )
              )
            )
          )
        )
      )

    lines_out.saveAsTextFile("rem/test");

    sc.stop();

  }

}
