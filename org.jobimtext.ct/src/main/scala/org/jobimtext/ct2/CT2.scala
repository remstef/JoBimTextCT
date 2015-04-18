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

package org.jobimtext.ct2

import java.text.DecimalFormat

import org.jobimtext.util.Util

/**
 * Created by Steffen Remus.
 */
object CT2 {

  val EMPTY_CT = CT2("","",0d,0d,0d,0d,0d,0d,0d,0l)

  def fromString(ct2AsString:String):CT2 = {
    ct2AsString.split("\t") match {
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22,ndocs) => CT2(u1,u2,n11.toDouble,n12.toDouble,n21.toDouble,n22.toDouble,o12.toDouble,o21.toDouble,o22.toDouble,ndocs.toLong)
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22) => CT2(u1,u2,n11.toDouble,n12.toDouble,n21.toDouble,n22.toDouble,o12.toDouble,o21.toDouble,o22.toDouble,1l)
      case _ => EMPTY_CT
    }
  }

  def fromStringArray(ct2AsStringArray:Array[String]):CT2 = {
    ct2AsStringArray match {
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22,ndocs) => CT2(u1,u2,n11.toDouble,n12.toDouble,n21.toDouble,n22.toDouble,o12.toDouble,o21.toDouble,o22.toDouble, ndocs.toLong)
      case  Array(u1,u2,n11,n12,n21,n22,o12,o21,o22) => CT2(u1,u2,n11.toDouble,n12.toDouble,n21.toDouble,n22.toDouble,o12.toDouble,o21.toDouble,o22.toDouble,1l)
      case _ => EMPTY_CT
    }
  }

}


case class CT2(u1:String, u2:String, var n11:Double, var n12:Double, var n21:Double, var n22:Double, var o12:Double, var o21:Double, var o22:Double, var ndocs:Long) {

  def n1dot = n11 + n12
  def ndot1 = n11 + n21
  def n = n11 + n12 + n21 + n22

  def o11 = 1d
  def o1dot = o11 + o12
  def odot1 = o11 + o21
  def o = o11 + o12 + o21 + o22

  def +(b:CT2):CT2 =
    CT2(u1,u2,
      n11+b.n11,
      n12+b.n12,
      n21+b.n21,
      n22+b.n22,
      o12+b.o12,
      o21+b.o21,
      o22+b.o22,
      ndocs+b.ndocs)


  def +=(ct2:CT2):CT2 = {
    n11+=ct2.n11
    n12+=ct2.n12
    n21+=ct2.n21
    n22+=ct2.n22
    o12+=ct2.o12
    o21+=ct2.o21
    o22+=ct2.o22
    ndocs+=ndocs
    this
  }

  override def toString():String = {
    "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d".format(
      u1,u2,
      Util.format(n11),
      Util.format(n12),
      Util.format(n21),
      Util.format(n22),
      Util.format(o12),
      Util.format(o21),
      Util.format(o22),ndocs)
  }

}
