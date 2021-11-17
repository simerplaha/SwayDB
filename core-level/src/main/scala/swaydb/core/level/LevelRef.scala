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

package swaydb.core.level

import swaydb.config.compaction.LevelMeter
import swaydb.config.{ForceSave, MMAP}
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.data.KeyValue
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.slice.{Slice, SliceOption}
import swaydb.{Bag, IO}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
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
   * Returns the first enabled [[MMAP.Log]] setting from first encountered PersistentLevel.
   */
  def getMmapForLogOrDisable(level: Option[LevelRef]): MMAP.Log = {
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
            MMAP.Off(ForceSave.Off)
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

  def get(key: Slice[Byte],
          readState: ThreadReadState): KeyValue.PutOption

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState): KeyValue.PutOption

  def floor(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean

  def lower(key: Slice[Byte],
            readState: ThreadReadState): KeyValue.PutOption

  def higher(key: Slice[Byte],
             readState: ThreadReadState): KeyValue.PutOption

  def headKey(readState: ThreadReadState): SliceOption[Byte]

  def lastKey(readState: ThreadReadState): SliceOption[Byte]

  def keyValueCount: Int

  def isEmpty: Boolean

  def segmentsCount(): Int

  def segmentFilesOnDisk: Seq[Path]

  def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit

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

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean

  def getSegment(minKey: Slice[Byte]): SegmentOption

  def existsOnDisk: Boolean

  def sizeOfSegments: Long

  def blockCacheSize(): Option[Long]

  def cachedKeyValuesSize(): Option[Long]

  def close[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

  def closeNoSweep: IO[swaydb.Error.Level, Unit]

  def closeSegments(): IO[swaydb.Error.Level, Unit]

  def meterFor(levelNumber: Int): Option[LevelMeter]

  def levelNumber: Int

  def isZero: Boolean

  def stateId: Long

  def nextCompactionDelay: FiniteDuration

  def delete[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

  def hasMMAP: Boolean =
    LevelRef.hasMMAP(this)

  override def equals(obj: Any): Boolean =
    obj match {
      case other: LevelRef =>
        this.rootPath == other.rootPath

      case _ =>
        false
    }

  override def hashCode(): Int =
    this.rootPath.hashCode()
}
