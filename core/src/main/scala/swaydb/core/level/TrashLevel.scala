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

import java.nio.file.{Path, Paths}

import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.Segment
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

private[core] object TrashLevel extends NextLevel {

  override val paths: PathsDistributor = PathsDistributor(Seq(), () => Seq())

  override val appendixPath: Path = Paths.get("Trash level has no path")

  override val rootPath: Path = Paths.get("Trash level has no path")

  override val throttle: LevelMeter => Throttle =
    (_) => Throttle(Duration.Zero, 0)

  override val nextLevel: Option[NextLevel] =
    None

  override val segmentsInLevel: Iterable[Segment] =
    Iterable.empty

  override val hasNextLevel: Boolean =
    false

  override val bloomFilterKeyValueCount: IO[swaydb.Error.Segment, Int] =
    IO.zero

  override val segmentsCount: Int =
    0

  override val segmentFilesOnDisk: List[Path] =
    List.empty

  override def take(count: Int): Slice[Segment] =
    Slice.create[Segment](0)

  override def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    ()

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    false

  override def getSegment(minKey: Slice[Byte]): Option[Segment] =
    None

  override val existsOnDisk =
    false

  override val levelSize: Long =
    0

  override val head =
    IO.Defer.none

  override val last =
    IO.Defer.none

  override def get(key: Slice[Byte]) =
    IO.Defer.none

  override def lower(key: Slice[Byte]) =
    IO.Defer.none

  override def higher(key: Slice[Byte]) =
    IO.Defer.none

  override val isEmpty: Boolean =
    true

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    Iterable.empty

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    Iterable.empty

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    Iterable.empty

  override val sizeOfSegments: Long =
    0

  override def releaseLocks: IO[swaydb.Error.Close, Unit] =
    IO.unit

  override val close: IO[swaydb.Error.Close, Unit] =
    IO.unit

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    None

  override def mightContainKey(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    IO.`false`

  override def mightContainFunction(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    IO.`false`

  override val isTrash: Boolean = true

  override def ceiling(key: Slice[Byte]): IO.Defer[swaydb.Error.Segment, Option[KeyValue.ReadOnly.Put]] =
    IO.Defer.none

  override def floor(key: Slice[Byte]): IO.Defer[swaydb.Error.Segment, Option[KeyValue.ReadOnly.Put]] =
    IO.Defer.none

  override val headKey: IO.Defer[swaydb.Error.Segment, Option[Slice[Byte]]] =
    IO.Defer.none

  override val lastKey: IO.Defer[swaydb.Error.Segment, Option[Slice[Byte]]] =
    IO.Defer.none

  override def closeSegments(): IO[swaydb.Error.Segment, Unit] =
    IO.unit

  override def levelNumber: Int = -1

  override def inMemory: Boolean = true

  override def isCopyable(map: swaydb.core.map.Map[Slice[Byte], Memory.SegmentResponse]): Boolean =
    true

  override def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment]) =
    (segments, Iterable.empty)

  override def put(segment: Segment)(implicit ec: ExecutionContext): IO[Nothing, IO.Right[Nothing, Unit]] =
    IO.unitUnit

  override def put(map: swaydb.core.map.Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Unit]] =
    IO.unitUnit

  override def put(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Unit]] =
    IO.unitUnit

  override def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Segment, Int] =
    IO.Right(segments.size)

  override val meter: LevelMeter =
    new LevelMeter {
      override def segmentsCount: Int = 0
      override def levelSize: Long = 0
      override def requiresCleanUp: Boolean = false
      override def segmentCountAndLevelSize: (Int, Long) = (segmentsCount, levelSize)
      override def nextLevelMeter: Option[LevelMeter] = None
    }

  override def refresh(segment: Segment)(implicit ec: ExecutionContext): IO[Nothing, IO.Right[Nothing, Unit]] =
    IO.unitUnit

  override def collapse(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO[Nothing, IO[Error.Segment, Int]] =
    IO.Right[Nothing, IO[Error.Segment, Int]](IO.Right(segments.size))(IO.ExceptionHandler.Nothing)

  override def isZero: Boolean =
    false

  override def nextCompactionDelay: FiniteDuration =
    365.days

  override def nextThrottlePushCount: Int =
    0
  override def segmentSize: Long = 0

  override def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment]) =
    Level.emptySegmentsToPush

  override def optimalSegmentsToCollapse(take: Int): Iterable[Segment] =
    Segment.emptyIterable

  override def groupBy: Option[GroupByInternal.KeyValues] =
    None

  override def isUnreserved(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    true

  override def isUnreserved(segment: Segment): Boolean =
    true

  override def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    true

  override def delete: IO[swaydb.Error.Delete, Unit] = IO.unit
}
