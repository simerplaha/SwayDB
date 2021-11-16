/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.java

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.util.Java._
import swaydb.utils.Java._
import swaydb.{Apply, Expiration, Glass, Prepare, PureFunction}

import java.nio.file.Path
import java.util.Optional
import java.{lang, util}
import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class Set[A, F](asScala: swaydb.Set[A, F, Glass])(implicit evd: F <:< PureFunction[A, Nothing, Apply.Set[Nothing]]) extends Source[A, A] {

  def path: Path =
    asScala.path

  def get(elem: A): Optional[A] =
    asScala.get(elem).asJava

  def contains(elem: A): Boolean =
    asScala.contains(elem)

  def mightContain(elem: A): Boolean =
    asScala.mightContain(elem)

  def mightContainFunction(function: F): Boolean =
    asScala.mightContainFunction(function)

  def add(elem: A): Unit =
    asScala add elem

  def add(elem: A, expireAfter: java.time.Duration): Unit =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: java.lang.Iterable[A]): Unit =
    asScala.add(elems.asScala)

  def add(elems: Stream[A]): Unit =
    asScala.add(elems.asScalaStream)

  def add(elems: java.util.Iterator[A]): Unit =
    asScala.add(elems.asScala)

  def remove(elem: A): Unit =
    asScala.remove(elem)

  def remove(from: A, to: A): Unit =
    asScala.remove(from, to)

  def remove(elems: java.lang.Iterable[A]): Unit =
    asScala.remove(elems.asScala)

  def remove(elems: Stream[A]): Unit =
    asScala.remove(elems.asScalaStream)

  def remove(elems: java.util.Iterator[A]): Unit =
    asScala.remove(elems.asScala)

  def expire(elem: A, after: java.time.Duration): Unit =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): Unit =
    asScala.expire(from, to, after.toScala)

  def expire(elems: java.lang.Iterable[Pair[A, java.time.Duration]]): Unit =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: Stream[Pair[A, java.time.Duration]]): Unit =
    asScala.expire {
      elems.asScalaStream.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: java.util.Iterator[Pair[A, java.time.Duration]]): Unit =
    asScala.expire {
      elems.asScala map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def clear(): Unit =
    asScala.clear()

  def applyFunction(from: A, to: A, function: F): Unit =
    asScala.applyFunction(from, to, function)

  def applyFunction(elem: A, function: F): Unit =
    asScala.applyFunction(elem, function)

  def commit(prepare: java.lang.Iterable[Prepare[A, Void, F]]): Unit =
    asScala.commit(prepare.asScala.asInstanceOf[Iterable[Prepare[A, Nothing, F]]])

  def commit(prepare: Stream[Prepare[A, Void, F]]): Unit =
    asScala.commit(prepare.asInstanceOf[Stream[Prepare[A, Nothing, F]]].asScalaStream)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def blockCacheSize(): Option[Long] =
    asScala.blockCacheSize()

  def cachedKeyValuesSize(): Option[Long] =
    asScala.cachedKeyValuesSize()

  def openedFiles(): Option[Long] =
    asScala.openedFiles()

  def pendingDeletes(): Option[Long] =
    asScala.pendingDeletes()

  def elemSize(elem: A): Int =
    asScala.elemSize(elem)

  def expiration(elem: A): Optional[Expiration] =
    asScala.expiration(elem).asJavaMap(_.asJava)

  def timeLeft(elem: A): Optional[java.time.Duration] =
    asScala.timeLeft(elem).asJavaMap(_.toJava)

  override def head: Optional[A] =
    asScala.head.asJava

  def clearAppliedFunctions(): lang.Iterable[String] =
    asScala.clearAppliedFunctions().asJava

  def clearAppliedAndRegisteredFunctions(): lang.Iterable[String] =
    asScala.clearAppliedAndRegisteredFunctions().asJava

  def isFunctionApplied(function: F): Boolean =
    asScala.isFunctionApplied(function)

  def sizeOfBloomFilterEntries: Int =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: Boolean =
    asScala.isEmpty

  def nonEmpty: Boolean =
    asScala.nonEmpty

  override def last: Optional[A] =
    asScala.last.asJava

  def asJava: util.Set[A] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def asScalaStream: swaydb.Source[A, A, Glass] =
    asScala

  override def equals(other: Any): Boolean =
    other match {
      case other: Set[_, _] =>
        other.asScala.equals(this.asScala)

      case _ =>
        false
    }

  override def hashCode(): Int =
    asScala.hashCode()

  override def toString(): String =
    asScala.toString()
}
