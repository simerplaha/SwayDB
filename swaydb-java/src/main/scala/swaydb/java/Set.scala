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
 */

package swaydb.java

import java.nio.file.Path
import java.util
import java.util.Optional

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._
import swaydb.{Apply, Bag, Pair}

import scala.collection.mutable.ListBuffer
import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._
import scala.collection.compat._

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class Set[A, F](private val _asScala: swaydb.Set[A, _, Bag.Less]) {

  implicit val exceptionHandler = swaydb.IO.ExceptionHandler.Throwable
  implicit val bag = Bag.less

  val asScala: swaydb.Set[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]], Bag.Less] =
    _asScala.asInstanceOf[swaydb.Set[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]], Bag.Less]]

  def path: Path =
    asScala.path

  def get(elem: A): Optional[A] =
    asScala.get(elem).asJava

  def contains(elem: A): Boolean =
    asScala.contains(elem)

  def mightContain(elem: A): Boolean =
    asScala.mightContain(elem)

  def mightContainFunction(function: A): Boolean =
    (asScala mightContainFunction function)

  def add(elem: A): swaydb.OK =
    asScala add elem

  def add(elem: A, expireAfter: java.time.Duration): swaydb.OK =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: java.util.List[A]): swaydb.OK =
    asScala.add(elems.asScala)

  def add(elems: Stream[A]): swaydb.OK =
    asScala.add(elems.asScala)

  def add(elems: java.util.Iterator[A]): swaydb.OK =
    asScala.add(elems.asScala.to(Iterable))

  def remove(elem: A): swaydb.OK =
    asScala.remove(elem)

  def remove(from: A, to: A): swaydb.OK =
    asScala.remove(from, to)

  def remove(elems: java.util.List[A]): swaydb.OK =
    asScala.remove(elems.asScala)

  def remove(elems: Stream[A]): swaydb.OK =
    asScala.remove(elems.asScala)

  def remove(elems: java.util.Iterator[A]): swaydb.OK =
    asScala.remove(elems.asScala.to(Iterable))

  def expire(elem: A, after: java.time.Duration): swaydb.OK =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): swaydb.OK =
    asScala.expire(from, to, after.toScala)

  def expire(elems: java.util.List[Pair[A, java.time.Duration]]): swaydb.OK =
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
      } to Iterable
    }

  def clear(): swaydb.OK =
    asScala.clear()

  def applyFunction(from: A, to: A, function: F): swaydb.OK =
    asScala.applyFunction(from, to, PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]]))

  def applyFunction(elem: A, function: F): swaydb.OK =
    asScala.applyFunction(elem, PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]]))

  def commit[P <: Prepare.Set[A, F]](prepare: java.util.List[P]): swaydb.OK =
    commit[P](prepare.iterator())

  def commit[P <: Prepare.Set[A, F]](prepare: Stream[P]): swaydb.OK =
    asScala.commit {
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[swaydb.Prepare[A, Nothing, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]]) {
          case (scala, java) =>
            val javaFunk = java.asInstanceOf[Prepare.Set[A, swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]]]
            scala += Prepare.toScala(javaFunk)
        }
    }

  def commit[P <: Prepare.Set[A, F]](prepare: java.util.Iterator[P]): swaydb.OK = {
    val prepareStatements =
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[swaydb.Prepare[A, Nothing, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]]) {
          case (scala, java) =>
            val javaFunk = java.asInstanceOf[Prepare.Set[A, swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]]]
            scala += Prepare.toScala(javaFunk)
        }

    asScala commit prepareStatements
  }

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

  def from(key: A): Set[A, F] =
    Set(asScala.from(key))

  def before(key: A): Set[A, F] =
    Set(asScala.before(key))

  def fromOrBefore(key: A): Set[A, F] =
    Set(asScala.fromOrBefore(key))

  def after(key: A): Set[A, F] =
    Set(asScala.after(key))

  def fromOrAfter(key: A): Set[A, F] =
    Set(asScala.fromOrAfter(key))

  def headOptional: Optional[A] =
    asScala.headOption.asJava

  def stream: Stream[A] =
    Stream.fromScala(asScala.stream)

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

  def lastOptional: Optional[A] =
    asScala.lastOption.asJava

  def reverse: Set[A, F] =
    Set(asScala.reverse)

  def asJava: util.Set[A] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  private def copy(): Unit = ()

  override def toString(): String =
    asScala.toString()
}
