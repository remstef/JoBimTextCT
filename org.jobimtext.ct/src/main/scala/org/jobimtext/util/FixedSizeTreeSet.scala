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

package org.jobimtext.util

import scala.collection.TraversableOnce
import scala.collection.mutable.TreeSet


/**
 *
 * A fixed size TreeSet
 *
 * Keeps only the n smallest entries according to Ordering
 *
 * Created by Steffen Remus.
 */
object FixedSizeTreeSet {

  def empty[A](implicit ordering: Ordering[A], maxsize:Int) = new FixedSizeTreeSet[A](maxsize)(ordering)

}

class FixedSizeTreeSet[A](val maxsize:Int)(override implicit val ordering: Ordering[A]) extends TreeSet[A] {

  override def +=(elem: A): this.type = {
    if(contains(elem))
      return this
    if (size >= maxsize) {
      if (compare(last, elem) < 0)
        return this
      super.-=(last)
    }
    return super.+=(elem)
  }


  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.seq.foreach(this += _)
    return this
  }

}
