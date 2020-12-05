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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level

import java.nio.file.{Path, Paths}
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.segment.{Segment, SegmentOption, ThreadReadState}
import swaydb.data.compaction.{LevelMeter, ParallelMerge, Throttle}
import swaydb.data.config.PushForwardStrategy
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.{Error, IO}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

private[core] object TrashLevel extends NextLevel {

  override val pathDistributor: PathsDistributor = PathsDistributor(Seq(), () => Seq())

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

  override val keyValueCount: Int =
    0

  override val segmentsCount: Int =
    0

  override val segmentFilesOnDisk: List[Path] =
    List.empty

  override def take(count: Int): Slice[Segment] =
    Slice.of[Segment](0)

  override def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    ()

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    false

  override def getSegment(minKey: Slice[Byte]): SegmentOption =
    Segment.Null

  override val existsOnDisk =
    false

  override val levelSize: Long =
    0

  override def head(readState: ThreadReadState) =
    KeyValue.Put.Null

  override def last(readState: ThreadReadState) =
    KeyValue.Put.Null

  override def get(key: Slice[Byte], readState: ThreadReadState) =
    KeyValue.Put.Null

  override def lower(key: Slice[Byte], readState: ThreadReadState) =
    KeyValue.Put.Null

  override def higher(key: Slice[Byte], readState: ThreadReadState) =
    KeyValue.Put.Null

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

  override val closeNoSweep: IO[swaydb.Error.Close, Unit] =
    IO.unit

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    None

  override def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean =
    false

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    false

  override val isTrash: Boolean = true

  override def ceiling(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
    KeyValue.Put.Null

  override def floor(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
    KeyValue.Put.Null

  override def headKey(readState: ThreadReadState): SliceOption[Byte] =
    Slice.Null

  override def lastKey(readState: ThreadReadState): SliceOption[Byte] =
    Slice.Null

  override def closeSegments(): IO[swaydb.Error.Segment, Unit] =
    IO.unit

  override def levelNumber: Int = -1

  override def inMemory: Boolean = true

  override def isCopyable(map: swaydb.core.map.Map[Slice[Byte], Memory, LevelZeroMapCache]): Boolean =
    true

  override def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment]) =
    (segments, Iterable.empty)

  /**
   * Return empty Set here because it's Trash level and does not require compaction.
   */
  override def put(segment: Segment,
                   parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[Nothing, IO.Right[Nothing, Set[Int]]] =
    IO.Right(IO.Right(Set.empty[Int])(IO.ExceptionHandler.Nothing))(IO.ExceptionHandler.Nothing)

  override def put(map: swaydb.core.map.Map[Slice[Byte], Memory, LevelZeroMapCache],
                   parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Set[Int]]] =
    IO.Right(IO.Right(Set.empty[Int])(IO.ExceptionHandler.Nothing))(IO.ExceptionHandler.Nothing)

  override def put(segments: Iterable[Segment],
                   parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Set[Int]]] =
    IO.Right(IO.Right(Set.empty[Int])(IO.ExceptionHandler.Nothing))(IO.ExceptionHandler.Nothing)

  override def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Segment, Int] =
    IO.Right(segments.size)

  override val meter: LevelMeter =
    new LevelMeter {
      override def segmentsCount: Int = 0
      override def levelSize: Long = 0
      override def requiresCleanUp: Boolean = false
      override def segmentCountAndLevelSize: (Int, Long) = (segmentsCount, levelSize)
      override def nextLevelMeter: Option[LevelMeter] = None
      override def pushForwardStrategy: PushForwardStrategy = TrashLevel.pushForwardStrategy
    }

  override def refresh(segment: Segment): IO[Nothing, IO.Right[Nothing, Unit]] =
    IO.unitUnit

  override def collapse(segments: Iterable[Segment],
                        parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[Nothing, IO[Error.Segment, Int]] =
    IO.Right[Nothing, IO[Error.Segment, Int]](IO.Right(segments.size))(IO.ExceptionHandler.Nothing)

  override def isZero: Boolean =
    false

  override def nextCompactionDelay: FiniteDuration =
    365.days

  override def nextThrottlePushCount: Int =
    0
  override def minSegmentSize: Int = 0

  override def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment]) =
    Level.emptySegmentsToPush

  override def optimalSegmentsToCollapse(take: Int): Iterable[Segment] =
    Segment.emptyIterable

  def lastSegmentId: Option[Long] =
    None

  override def stateId: Long =
    0

  override def isUnreserved(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    true

  override def isUnreserved(segment: Segment): Boolean =
    true

  override def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    true

  override def deleteNoSweep: IO[swaydb.Error.Delete, Unit] = IO.unit

  override def closeNoSweepNoRelease(): IO[Error.Level, Unit] =
    IO.unit

  override def deleteNoSweepNoClose(): IO[Error.Level, Unit] =
    IO.unit

  def close[BAG[_]]()(implicit bag: swaydb.Bag[BAG]): BAG[Unit] =
    bag.unit

  def delete[BAG[_]]()(implicit bag: swaydb.Bag[BAG]): BAG[Unit] =
    bag.unit

  override def isNonEmpty(): Boolean =
    true

  override def pushForwardStrategy: PushForwardStrategy =
    PushForwardStrategy.On
}
