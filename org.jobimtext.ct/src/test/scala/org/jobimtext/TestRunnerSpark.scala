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

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jobimtext._
import org.jobimtext.ct2.{CT2, AggregateCT, ClassicToCT}
import org.jobimtext.extract.{NgramWithHole, CooccurrenceWindow}
import org.jobimtext.misc._
import org.jobimtext.sim._
import org.jobimtext.util.FixedSizeTreeSet

/**
 * Created by Steffen Remus.
 */
object TestRunnerSpark {

  def main(args: Array[String]) {
    testSampleSentences
//    testArtificalData
  }


  def testSampleSentences {

    val conf = new SparkConf()
      .setAppName("SparkTestRunner")
      .setMaster("local[*]")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[FixedSizeTreeSet[_]], classOf[CT2]))
//      .set("spark.kryo.classesToRegister", "org.jobimtext.util.FixedSizeTreeSet,org.jobimtext.ct2.CT2")


    val sc = new SparkContext(conf);

    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/samplesentences_2.txt").filter(_.nonEmpty).repartition(sc.defaultParallelism)
//    val lines_out =
//      SimSortTopN(10,false,
//        KLDivergenceRdcBy(
//          JoinBySharedFeaturesGrpBy(-1,
//            TopProbs(1000,
//              ct2.ProbsFromCT(
//                ct2.SumMarginalsCT(
//                  ct2.AggregateCT.classic(
//                    ClassicToCT(
//                      CooccurrenceWindow(3,lines_in)
//                    )
//                  )
//                )
//              )
//            )
//          )
//        )
//      )

    val lines_out =
//      TakeTopN(10,true,true,
//        KLDivergenceRdcBy(
        FreqSim(with_features = true,
          JoinBySharedFeaturesGrpBy(-1,
            TakeTopN(100, true, false,
               ct2.sig.LMIFromCT(
                  ct2.AggregateCT.classic(Ctconf.default,
                    ClassicToCT(
//                      CooccurrenceWindow(100,lines_in)
                      NgramWithHole(3,false,lines_in)
                    )
                  )
                )
            )
          )
        )
//      )
    //lines_out.saveAsTextFile("org.jobimtext.ct/local_data/samplesentences_kls");


//    val lines_out = SimSortTopN(10,false,sc.textFile("org.jobimtext.ct/local_data/samplesent_kl"))
//    lines_out.saveAsTextFile("org.jobimtext.ct/local_data/samplesent_kls");

    lines_out.sortBy(x => x).foreach(line => println(line));

    sc.stop();
  }

  def testArtificalData {

    val conf = new SparkConf()
      .setAppName("SparkTestRunner")
      .setMaster("local[*]")
      .set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[FixedSizeTreeSet[_]], classOf[CT2]))


    val prunconf = Ctconf(min_n11 = 1, min_n1dot = 1, min_ndot1 = 1,min_odot1 = 1)

    val sc = new SparkContext(conf);

//    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/artificial-ct.txt").filter(_.nonEmpty)
//    val lines_out = AggregateCT2(lines_in)
//    val lines_out = AggregateCT2.classic(lines_in)


    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/artificial-jb.txt").filter(_.nonEmpty)

//        val lines_out = ClassicToCT(lines_in)
//    val lines_out = ct2.AggregateCT(ClassicToCT(lines_in));
//    val lines_out = ct2.AggregateCT.classic(ClassicToCT(lines_in));
//    val lines_out = AggregateCT(2, ClassicToCT(lines_in));
//    val lines_out = ClassicToCT.classicToAggregatedCT2(lines_in)

//    val lines_in = sc.textFile("org.jobimtext.ct/src/test/files/artificial-jb-wfc.txt").filter(_.nonEmpty)
//    val lines_out = ClassicToCT.classicWordFeatureCountToAggregatedCT2(lines_in)

    val lines_out =
//      SimSortTopN(1,false,
//        KLDivergenceRdcBy(
//          JoinBySharedFeaturesGrpBy(-1,
////              JoinBySharedFeaturesCartesian(
//            TakeTopN(1,true,
//              ct2.ProbsFromCT(
//                Prune.pruneCT(prunconf.filterCT,
                  ct2.AggregateCT.classic(Ctconf.default,
                    ClassicToCT(lines_in)
                  )
//                )
//              )
//            )
//          )
//        )
//      )

//    //lines_out.saveAsTextFile("org.jobimtext.ct/local_data/testout");
//    lines_out.collect().foreach(line => println(line));
    lines_out.takeSample(withReplacement = false, num = 100, seed = 42l).foreach(line => println(line))

    sc.stop();
  }

}
