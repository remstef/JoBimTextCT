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

/**
 *
 * Example Scala / Spark Shell Script to configure and execute a jbtct pipeline
 *
 * Execute:
 *    spark-shell --jars <jbtct.jar-file>[,more-jars] [<spark-options>] -i <script.scala>
 *
 * Execute within spark Shell:
 *    $ scala> :load <jbtct-pipeline-short.scala>
 *
 * Example:
 *
 *    spark-shell --jars jbtct.jar -i jbtct-pipeline-short.scala
 *    spark-shell --master local[*] --conf spark.ui.port=4041  --num-executors 4 --jars jbtct.jar  -i jbtct-pipeline-short.scala
 *    spark-shell --master yarn --num-executors 100 --queue testing --conf spark.default.parallelism=300 --jars jbtct.jar -i jbtct-pipeline-short.scala
 *    spark-shell --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.classesToRegister=org.jobimtext.util.FixedSizeTreeSet,org.jobimtext.ct2.CT2  --conf spark.default.parallelism=42 --master local[*] --conf spark.ui.port=4041  --num-executors 16 --executor-cores 1 --executor-memory 2G --driver-memory 2G --jars jbtct/ct-0.0.1.jar -i jbtct/jbtct-pipeline-short.scala
 *
 * Configurations:
 *
 *    min_ndot1    == -f       (2)                    minimum feature count
 *    min_n1dot    == -w       (2)                    minimum word count
 *    min_n11      == -wf      (2) [classic 0]        minimum word feature count
 *    max_odot1    == -wpfmax  (1000)                 maximum features per word
 *    min_odot1    == -wpfmin  (2)                    minimum features per word
 *    min_docs                 (1)                    minimum document count
 *
 *    min_sig      == -s       (0)                    minimum word-feature significance value
 *    topn_f       == -p       (1000)                 top n features
 *    topn_s       == -t       (200)                  top n similar
 *    min_s        == -ms      (2)                    minimum word-word similarity value
 *
 */

try {

//  import relevant stuff
import org.apache.spark.{SparkConf, SparkContext}
import org.jobimtext.{ct2, sim}
import org.jobimtext.extract._
import org.jobimtext.misc._
import org.jobimtext.util.FixedSizeTreeSet

// in case, uncomment
val sc = new SparkContext(new SparkConf())

// set the name of the app
sc.getConf
  .setAppName("jbtct")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(classOf[FixedSizeTreeSet[_]], classOf[ct2.CT2]))

val ctconf = Ctconf(
  min_ndot1 = 2,
  min_n1dot = 2,
  min_n11 = 2,
  max_odot1 = 1000,
  min_odot1 = 2,
  min_docs = 1,
  min_sig = 0, // Double.NegativeInfinity
  topn_f = 1000,
  topn_s = 200,
  min_sim = 2
)

// set input and output paths
val in = "src/test/files/samplesentences_2.txt" //"input-dir-or-file" //
val out = "out2-samplesentences2.txt" // "output-dir" //

// read non empty lines from input dir or file
val lines_in = sc.textFile(in).filter(_.nonEmpty).repartition(sc.defaultParallelism)

val lines_out =
  TakeTopN(n = ctconf.topn_s, descending = true, sortbykey = true,
    Prune.pruneByValue(filterfun = ctconf.filterBySimilarityScore,
      sim.FreqSim( // KLDivergenceRdcBy(
        JoinBySharedFeaturesGrpBy(prune = -1,
          TakeTopN(n = ctconf.topn_f, descending = true, sortbykey = false,
            Prune.pruneByValue(filterfun = ctconf.filterBySignificance,
              ct2.sig.LMIFromCT( // ct2.sig.FreqFromCT(ctsp) // ct2.sig.ProbsFromCT(ctsp)
                ct2.AggregateCT.classic(ctconf,
                  ct2.ClassicToCT(
                    NgramWithHole(n = 3, allcombinations = true,   // CooccurrenceWindow(100,lines_in)
                      lines_in
                    ).repartition(sc.defaultParallelism)
                  )
                )
              )
            )
          ).repartition(sc.defaultParallelism)
        ).repartition(sc.defaultParallelism)
      ).repartition(sc.defaultParallelism)
    )
  )
  .saveAsTextFile(out)

}catch{
  case e:Throwable =>
    e.printStackTrace()
    println("Something's wrong! (%s: %s)".format(e.getClass.getSimpleName, e.getMessage))
}

System.exit(0)
