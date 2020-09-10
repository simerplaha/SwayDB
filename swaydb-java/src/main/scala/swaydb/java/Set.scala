/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java

import java.nio.file.Path
import java.util.Optional
import java.{lang, util}

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._
import swaydb.{Apply, Bag, Pair, Prepare}

import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class Set[A, F](asScala: swaydb.Set[A, F, Bag.Less])(implicit evd: F <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]) {

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

  def add(elem: A): swaydb.OK =
    asScala add elem

  def add(elem: A, expireAfter: java.time.Duration): swaydb.OK =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: java.lang.Iterable[A]): swaydb.OK =
    asScala.add(elems.asScala)

  def add(elems: Stream[A]): swaydb.OK =
    asScala.add(elems.asScala)

  def add(elems: java.util.Iterator[A]): swaydb.OK =
    asScala.add(elems.asScala)

  def remove(elem: A): swaydb.OK =
    asScala.remove(elem)

  def remove(from: A, to: A): swaydb.OK =
    asScala.remove(from, to)

  def remove(elems: java.lang.Iterable[A]): swaydb.OK =
    asScala.remove(elems.asScala)

  def remove(elems: Stream[A]): swaydb.OK =
    asScala.remove(elems.asScala)

  def remove(elems: java.util.Iterator[A]): swaydb.OK =
    asScala.remove(elems.asScala)

  def expire(elem: A, after: java.time.Duration): swaydb.OK =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): swaydb.OK =
    asScala.expire(from, to, after.toScala)

  def expire(elems: java.lang.Iterable[Pair[A, java.time.Duration]]): swaydb.OK =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: Stream[Pair[A, java.time.Duration]]): swaydb.OK =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: java.util.Iterator[Pair[A, java.time.Duration]]): swaydb.OK =
    asScala.expire {
      elems.asScala map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def clear(): swaydb.OK =
    asScala.clear()

  def applyFunction(from: A, to: A, function: F): swaydb.OK =
    asScala.applyFunction(from, to, function)

  def applyFunction(elem: A, function: F): swaydb.OK =
    asScala.applyFunction(elem, function)

  def commit(prepare: java.lang.Iterable[Prepare[A, Void, F]]): swaydb.OK =
    asScala.commit(prepare.asScala.asInstanceOf[Iterable[Prepare[A, Nothing, F]]])

  def commit(prepare: Stream[Prepare[A, Void, F]]): swaydb.OK =
    asScala.commit(prepare.asInstanceOf[Stream[Prepare[A, Nothing, F]]].asScala)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def elemSize(elem: A): Int =
    asScala.elemSize(elem)

  def expiration(elem: A): Optional[Deadline] =
    asScala.expiration(elem).asJavaMap(_.asJava)

  def timeLeft(elem: A): Optional[java.time.Duration] =
    asScala.timeLeft(elem).asJavaMap(_.toJava)

  def head: Optional[A] =
    asScala.head.asJava

  def clearAppliedFunctions(): lang.Iterable[String] =
    asScala.clearAppliedFunctions().asJava

  def clearAppliedAndRegisteredFunctions(): lang.Iterable[String] =
    asScala.clearAppliedAndRegisteredFunctions().asJava

  def isFunctionApplied(function: F): Boolean =
    asScala.isFunctionApplied(function)

  def stream: Source[A, A] =
    new Source(asScala.stream)

  def iterator: java.util.Iterator[A] =
    asScala
      .iterator(Bag.less)
      .asJava

  def sizeOfBloomFilterEntries: Int =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: Boolean =
    asScala.isEmpty

  def nonEmpty: Boolean =
    asScala.nonEmpty

  def last: Optional[A] =
    asScala.last.asJava

  def asJava: util.Set[A] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def toString(): String =
    asScala.toString()
}
