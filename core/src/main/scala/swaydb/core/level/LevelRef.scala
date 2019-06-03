/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.level

import java.nio.file.Path

import swaydb.core.data.KeyValue
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object LevelRef {

  @tailrec
  def firstPersistentLevel(level: Option[LevelRef]): Option[LevelRef] =
    level match {
      case Some(level) =>
        if (level.inMemory)
          firstPersistentLevel(level.nextLevel)
        else
          Some(level)
      case None =>
        None
    }

  def firstPersistentPath(level: Option[LevelRef]): Option[Path] =
    firstPersistentLevel(level).map(_.rootPath)

  def hasMMAP(level: Option[LevelRef]): Boolean =
    firstPersistentLevel(level) exists {
      case level: Level =>
        level.mmapSegmentsOnRead || level.mmapSegmentsOnWrite

      case _: LevelZero =>
        //not true. LevelZero can also be mmap.
        false
    }

  def getLevels(level: LevelRef): List[LevelRef] = {
    @tailrec
    def getLevels(level: Option[LevelRef], levels: List[LevelRef]): List[LevelRef] =
      level match {
        case Some(level) =>
          getLevels(level.nextLevel, levels :+ level)

        case None =>
          levels
      }

    getLevels(Some(level), List.empty)
  }

  def getLevels(level: NextLevel): List[NextLevel] =
    getLevels(level: LevelRef) map (_.asInstanceOf[NextLevel])

  def foreach[T](level: LevelRef, f: LevelRef => T): Unit = {
    f(level)
    level.nextLevel foreach {
      nextLevel =>
        foreach(nextLevel, f)
    }
  }

  def foreachRight[T](level: LevelRef, f: LevelRef => T): Unit = {
    level.nextLevel foreach {
      nextLevel =>
        foreachRight(nextLevel, f)
    }
    f(level)
  }

  def foldLeft[T](level: LevelRef, initial: T, f: (T, LevelRef) => T): T = {
    var currentT = initial
    foreach(
      level = level,
      f =
        level =>
          currentT = f(currentT, level)
    )
    currentT
  }

  def foldRight[T](level: LevelRef, initial: T, f: (T, LevelRef) => T): T = {
    var currentT = initial
    foreachRight(
      level = level,
      f =
        level =>
          currentT = f(currentT, level)
    )
    currentT
  }

  def map[T](level: LevelRef, f: LevelRef => T): Seq[T] = {
    val buffer = ListBuffer.empty[T]
    foreach(
      level = level,
      f =
        level =>
          buffer += f(level)
    )
    buffer
  }

  def mapRight[T](level: LevelRef, f: LevelRef => T): Seq[T] = {
    val buffer = ListBuffer.empty[T]
    foreachRight(
      level = level,
      f =
        level =>
          buffer += f(level)
    )
    buffer
  }
}

private[core] trait LevelRef {

  def inMemory: Boolean

  def releaseLocks: IO[Unit]

  def nextLevel: Option[NextLevel]

  def hasNextLevel: Boolean

  def appendixPath: Path

  def rootPath: Path

  def head: IO.Async[Option[KeyValue.ReadOnly.Put]]

  def last: IO.Async[Option[KeyValue.ReadOnly.Put]]

  def get(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]]

  def ceiling(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]]

  def floor(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]]

  def mightContain(key: Slice[Byte]): IO[Boolean]

  def lower(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]]

  def higher(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]]

  def headKey: IO.Async[Option[Slice[Byte]]]

  def lastKey: IO.Async[Option[Slice[Byte]]]

  def bloomFilterKeyValueCount: IO[Int]

  def isEmpty: Boolean

  def segmentsCount(): Int

  def segmentFilesOnDisk: Seq[Path]

  def foreachSegment[T](f: (Slice[Byte], Segment) => T)

  def foreachLevel[T](f: LevelRef => T): Unit =
    LevelRef.foreach(this, f)

  def foldLeftLevels[T](initial: T)(f: (T, LevelRef) => T): T =
    LevelRef.foldLeft(this, initial, f)

  def mapLevels[T](f: LevelRef => T): Seq[T] =
    LevelRef.map(this, f)

  def foreachRightLevel[T](f: LevelRef => T): Unit =
    LevelRef.foreachRight(this, f)

  def foldRightLevels[T](initial: T)(f: (T, LevelRef) => T): T =
    LevelRef.foldRight(this, initial, f)

  def mapRightLevels[T](f: LevelRef => T): Seq[T] =
    LevelRef.mapRight(this, f)

  def reverseLevels: ListBuffer[LevelRef] = {
    val levels = ListBuffer.empty[LevelRef]
    LevelRef.foreachRight(
      level = this,
      f = level =>
        levels += level
    )
    levels
  }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean

  def getSegment(minKey: Slice[Byte]): Option[Segment]

  def existsOnDisk: Boolean

  def sizeOfSegments: Long

  def close: IO[Unit]

  def closeSegments(): IO[Unit]

  def meterFor(levelNumber: Int): Option[LevelMeter]

  def levelNumber: Long

  def isTrash: Boolean

  def isZero: Boolean

  def stateID: Long

  def nextCompactionDelay: FiniteDuration
}