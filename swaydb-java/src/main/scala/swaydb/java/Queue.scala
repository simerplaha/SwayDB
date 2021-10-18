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

import java.nio.file.Path
import java.util.Optional
import java.util.function.Supplier
import swaydb.Glass
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter

import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._

case class Queue[A](asScala: swaydb.Queue[A]) extends Stream[A] {

  def path: Path =
    asScala.path

  def push(elem: A): Unit =
    asScala.push(elem)

  def push(elem: A, expireAfter: java.time.Duration): Unit =
    asScala.push(elem, expireAfter.toScala)

  def push(elems: Stream[A]): Unit =
    asScala.push(elems.asScalaStream)

  def push(elems: java.lang.Iterable[A]): Unit =
    asScala.push(elems.asScala)

  def push(elems: java.util.Iterator[A]): Unit =
    asScala.push(elems.asScala)

  def pop(): Optional[A] =
    Optional.of(asScala.popOrNull())

  def popOrNull(): A =
    asScala.popOrNull()

  def popOrElse(orElse: Supplier[A]): A =
    asScala.popOrElse(orElse.get())

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def asScalaStream: swaydb.Stream[A, Glass] =
    asScala

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    asScala.levelMeter(levelNumber)

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

  override def equals(other: Any): Boolean =
    other match {
      case other: Queue[_] =>
        other.asScala.equals(this.asScala)

      case _ =>
        false
    }

  override def hashCode(): Int =
    asScala.hashCode()

  override def toString(): String =
    asScala.toString()

}
