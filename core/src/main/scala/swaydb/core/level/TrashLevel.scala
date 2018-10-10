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

import java.nio.file.{Path, Paths}

import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.core.util.TryUtil
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Success, Try}

private[core] object TrashLevel extends LevelRef {

  override val paths: PathsDistributor = PathsDistributor(Seq(), () => Seq())

  override val appendixPath: Path = Paths.get("Trash level has no path")

  override val rootPath: Path = Paths.get("Trash level has no path")

  override val throttle: LevelMeter => Throttle =
    (_) => Throttle(Duration.Zero, 0)

  override def !(request: LevelAPI): Unit =
    request match {
      case request: PushSegments =>
        request.replyTo ! PushSegmentsResponse(request, TryUtil.successUnit)
      case request: PushMap =>
        request.replyTo ! PushMapResponse(request, TryUtil.successUnit)
      case PullRequest(pullFrom) =>
        pullFrom ! Pull
    }

  override val nextLevel: Option[LevelRef] =
    None

  override val segmentsInLevel: Iterable[Segment] =
    Iterable.empty

  override val hasNextLevel: Boolean =
    false

  override val bloomFilterKeyValueCount: Try[Int] =
    Success(0)

  override val segmentsCount: Int =
    0

  override val segmentFilesOnDisk: List[Path] =
    List.empty

  override def take(count: Int): Slice[Segment] =
    Slice.create[Segment](0)

  override def foreach[T](f: (Slice[Byte], Segment) => T): Unit =
    ()

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    false

  override def getSegment(minKey: Slice[Byte]): Option[Segment] =
    None

  override val getBusySegments: List[Segment] =
    List.empty

  override val pushForward: Boolean =
    false

  override val nextBatchSize: Int =
    0

  /**
    * Forward only occurs if lower level is empty and not busy.
    */
  override def forward(levelAPI: LevelAPI): Try[Unit] =
    TryUtil.successUnit

  override def push(levelAPI: LevelAPI): Unit =
    this ! levelAPI

  override def nextPushDelayAndSegmentsCount: (FiniteDuration, Int) =
    (0.second, 0)

  override def nextBatchSizeAndSegmentsCount: (Int, Int) =
    (0, 0)

  override def nextPushDelayAndBatchSize: Throttle =
    Throttle(0.millisecond, 0)

  override def nextPushDelay: FiniteDuration =
    Duration.Zero

  override def removeSegments(segments: Iterable[Segment]): Try[Int] =
    Success(segments.size)

  override def put(segments: Iterable[Segment]): Try[Unit] =
    TryUtil.successUnit

  override def put(segment: Segment): Try[Unit] =
    TryUtil.successUnit

  override def pickSegmentsToPush(count: Int): Iterable[Segment] =
    Iterable.empty

  override def collapseAllSmallSegments(batch: Int): Try[Int] =
    Success(0)

  override val existsOnDisk =
    false

  override val levelSize: Long =
    0

  override val segmentCountAndLevelSize: (Int, Long) =
    (0, 0)

  override val head =
    TryUtil.successNone

  override val last =
    TryUtil.successNone

  override def get(key: Slice[Byte]) =
    TryUtil.successNone

  override def lower(key: Slice[Byte]) =
    TryUtil.successNone

  override def higher(key: Slice[Byte]) =
    TryUtil.successNone

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

  override def releaseLocks: Try[Unit] =
    TryUtil.successUnit

  override val close: Try[Unit] =
    TryUtil.successUnit

  override val meter: LevelMeter =
    LevelMeter(0, 0)

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    None

  override def mightContain(key: Slice[Byte]): Try[Boolean] =
    Success(false)

  override val isTrash: Boolean = true

  override def putMap(map: Map[Slice[Byte], Memory.SegmentResponse]): Try[Unit] =
    TryUtil.successUnit

  override def ceiling(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    TryUtil.successNone

  override def floor(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    TryUtil.successNone

  override val firstKey: Option[Slice[Byte]] =
    None

  override val lastKey: Option[Slice[Byte]] =
    None

  override def closeSegments(): Try[Unit] =
    TryUtil.successUnit

  override def clearExpiredKeyValues(): Try[Unit] =
    TryUtil.successUnit

  override val hasTimeLeftAtLeast: FiniteDuration =
    0.seconds
}
