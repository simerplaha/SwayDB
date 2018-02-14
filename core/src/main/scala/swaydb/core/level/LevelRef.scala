/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import swaydb.core.data.PersistentReadOnly
import swaydb.core.level.actor.{LevelAPI, LevelActorAPI}
import swaydb.core.segment.Segment
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice

import scala.util.Try

private[core] trait LevelRef extends LevelActorAPI {
  val paths: PathsDistributor
  val throttle: LevelMeter => Throttle

  def releaseLocks: Try[Unit]

  def !(request: LevelAPI): Unit

  def nextLevel: Option[LevelRef]

  def segments: Iterable[Segment]

  def hasNextLevel: Boolean

  def appendixPath: Path

  def rootPath: Path

  def takeSegments(size: Int,
                   condition: Segment => Boolean): Iterable[Segment]

  def head: Try[Option[PersistentReadOnly]]

  def last: Try[Option[PersistentReadOnly]]

  def get(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def mightContain(key: Slice[Byte]): Try[Boolean]

  def lower(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def higher(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def keyValueCount: Try[Int]

  def isEmpty: Boolean

  def segmentsCount(): Int

  def segmentFilesOnDisk: Seq[Path]

  def take(count: Int): Slice[Segment]

  def foreach[T](f: (Slice[Byte], Segment) => T)

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean

  def getSegment(minKey: Slice[Byte]): Option[Segment]

  def getBusySegments(): List[Segment]

  def takeSmallSegments(size: Int): Iterable[Segment]

  def takeLargeSegments(size: Int): Iterable[Segment]

  def existsOnDisk: Boolean

  def levelSize: Long

  def sizeOfSegments: Long

  def segmentCountAndLevelSize: (Int, Long)

  def close: Try[Unit]

  def meter: LevelMeter

  def meterFor(levelNumber: Int): Option[LevelMeter]

  def isTrash: Boolean

}
