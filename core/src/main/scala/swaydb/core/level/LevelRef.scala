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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import java.nio.file.Path
import scala.annotation.tailrec
import swaydb.core.data.KeyValue
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice

private[core] trait LevelRef {

  def inMemory: Boolean

  def paths: PathsDistributor

  def throttle: LevelMeter => Throttle

  def releaseLocks: IO[Unit]

  def !(request: LevelAPI): Unit

  def nextLevel: Option[LevelRef]

  def segmentsInLevel(): Iterable[Segment]

  def hasNextLevel: Boolean

  def appendixPath: Path

  def rootPath: Path

  def takeSegments(size: Int,
                   condition: Segment => Boolean): Iterable[Segment]

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

  def take(count: Int): Slice[Segment]

  def foreach[T](f: (Slice[Byte], Segment) => T)

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean

  def getSegment(minKey: Slice[Byte]): Option[Segment]

  def getBusySegments(): Iterable[Segment]

  def takeSmallSegments(size: Int): Iterable[Segment]

  def takeLargeSegments(size: Int): Iterable[Segment]

  def existsOnDisk: Boolean

  def levelSize: Long

  def sizeOfSegments: Long

  def segmentCountAndLevelSize: (Int, Long)

  def close: IO[Unit]

  def closeSegments(): IO[Unit]

  def meter: LevelMeter

  def meterFor(levelNumber: Int): Option[LevelMeter]

  def levelNumber: Long

  def isTrash: Boolean

}

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
}