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

package swaydb.core.level

import java.nio.file.Path

import swaydb.IO
import swaydb.core.data.KeyValue
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.{Segment, SegmentOption, ThreadReadState}
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.slice.Slice._


import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

object LevelRef {

  def firstPersistentLevel(level: Option[LevelRef]): Option[LevelRef] =
    level.flatMap(firstPersistentLevel)

  def firstPersistentLevel(level: LevelRef): Option[LevelRef] =
    if (level.inMemory)
      firstPersistentLevel(level.nextLevel)
    else
      Some(level)

  def hasMMAP(level: Option[LevelRef]): Boolean =
    level.exists(hasMMAP)

  def hasMMAP(level: LevelRef): Boolean =
    firstPersistentLevel(level) exists {
      case level: Level =>
        level.segmentConfig.mmap.mmapReads || level.segmentConfig.mmap.mmapWrites || level.appendix.mmap.hasMMAP

      case zero: LevelZero =>
        zero.mmap.hasMMAP
    }

  def hasDeleteAfterClean(level: Option[LevelRef]): Boolean =
    firstPersistentLevel(level) exists {
      case level: Level =>
        level.segmentConfig.mmap.deleteAfterClean

      case zero: LevelZero =>
        zero.mmap.deleteAfterClean
    }

  /**
   * Returns the first enabled [[MMAP.Map]] setting from first encountered PersistentLevel.
   */
  def getMmapForLogOrDisable(level: Option[LevelRef]): MMAP.Map = {
    val mmap =
      firstPersistentLevel(level) collectFirst {
        case level: Level if level.appendix.mmap.isMMAP =>
          level.appendix.mmap
      }

    mmap match {
      case Some(mmap) =>
        mmap

      case None =>
        firstPersistentLevel(level) collectFirst {
          case level: Level =>
            level.appendix.mmap
        } match {
          case Some(value) =>
            value

          case None =>
            MMAP.Disabled(ForceSave.Disabled)
        }
    }
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

  def map[T](level: LevelRef, f: LevelRef => T): Iterable[T] = {
    val buffer = ListBuffer.empty[T]
    foreach(
      level = level,
      f =
        level =>
          buffer += f(level)
    )
    buffer
  }

  def mapRight[T](level: LevelRef, f: LevelRef => T): Iterable[T] = {
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

  def releaseLocks: IO[swaydb.Error.Close, Unit]

  def nextLevel: Option[NextLevel]

  def hasNextLevel: Boolean

  def appendixPath: Path

  def rootPath: Path

  def head(readState: ThreadReadState): KeyValue.PutOption

  def last(readState: ThreadReadState): KeyValue.PutOption

  def get(key: Sliced[Byte],
          readState: ThreadReadState): KeyValue.PutOption

  def ceiling(key: Sliced[Byte],
              readState: ThreadReadState): KeyValue.PutOption

  def floor(key: Sliced[Byte],
            readState: ThreadReadState): KeyValue.PutOption

  def mightContainKey(key: Sliced[Byte]): Boolean

  def lower(key: Sliced[Byte],
            readState: ThreadReadState): KeyValue.PutOption

  def higher(key: Sliced[Byte],
             readState: ThreadReadState): KeyValue.PutOption

  def headKey(readState: ThreadReadState): SliceOption[Byte]

  def lastKey(readState: ThreadReadState): SliceOption[Byte]

  def keyValueCount: Int

  def isEmpty: Boolean

  def segmentsCount(): Int

  def segmentFilesOnDisk: Seq[Path]

  def foreachSegment[T](f: (Sliced[Byte], Segment) => T): Unit

  def foreachLevel[T](f: LevelRef => T): Unit =
    LevelRef.foreach(this, f)

  def foldLeftLevels[T](initial: T)(f: (T, LevelRef) => T): T =
    LevelRef.foldLeft(this, initial, f)

  def mapLevels[T](f: LevelRef => T): Iterable[T] =
    LevelRef.map(this, f)

  def foreachRightLevel[T](f: LevelRef => T): Unit =
    LevelRef.foreachRight(this, f)

  def foldRightLevels[T](initial: T)(f: (T, LevelRef) => T): T =
    LevelRef.foldRight(this, initial, f)

  def mapRightLevels[T](f: LevelRef => T): Iterable[T] =
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

  def containsSegmentWithMinKey(minKey: Sliced[Byte]): Boolean

  def getSegment(minKey: Sliced[Byte]): SegmentOption

  def existsOnDisk: Boolean

  def sizeOfSegments: Long

  def close()(implicit executionContext: ExecutionContext): Future[Unit]

  def closeNoSweep: IO[swaydb.Error.Level, Unit]

  def closeSegments(): IO[swaydb.Error.Level, Unit]

  def meterFor(levelNumber: Int): Option[LevelMeter]

  def levelNumber: Int

  def isTrash: Boolean

  def isZero: Boolean

  def stateId: Long

  def nextCompactionDelay: FiniteDuration

  def delete()(implicit executionContext: ExecutionContext): Future[Unit]

  def hasMMAP: Boolean =
    LevelRef.hasMMAP(this)
}
