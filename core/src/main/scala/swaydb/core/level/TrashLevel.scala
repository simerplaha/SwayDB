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

import swaydb.core.data.ValueType
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Success, Try}

private[core] object TrashLevel extends LevelRef {

  override val paths: PathsDistributor = PathsDistributor(Seq(), () => Seq())

  override def appendixPath: Path = Paths.get("Trash level has no path")

  def rootPath: Path = Paths.get("Trash level has no path")

  override val throttle: LevelMeter => Throttle =
    (_) => Throttle(Duration.Zero, 0)

  override def !(request: LevelAPI): Unit =
    request match {
      case request: PushSegments =>
        request.replyTo ! PushSegmentsResponse(request, Success())
      case request: PushMap =>
        request.replyTo ! PushMapResponse(request, Success())
      case PullRequest(pullFrom) =>
        pullFrom ! Pull
    }

  override def nextLevel: Option[LevelRef] =
    None

  override def segments: Iterable[Segment] =
    Iterable.empty

  override def hasNextLevel: Boolean =
    false

  override def keyValueCount: Try[Int] =
    Success(0)

  override def segmentsCount(): Int =
    0

  override def segmentFilesOnDisk: List[Path] =
    List.empty

  override def take(count: Int): Slice[Segment] =
    Slice.create[Segment](0)

  override def foreach[T](f: (Slice[Byte], Segment) => T): Unit =
    ()

  override def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    false

  override def getSegment(minKey: Slice[Byte]): Option[Segment] =
    None

  override def getBusySegments(): List[Segment] =
    List.empty

  override val pushForward: Boolean =
    false

  override def nextBatchSize: Int =
    0

  /**
    * Forward only occurs if lower level is empty and not busy.
    */
  override def forward(levelAPI: LevelAPI): Try[Unit] =
    Success()

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
    Success()

  override def put(segment: Segment): Try[Unit] =
    Success()

  override def pickSegmentsToPush(count: Int): Iterable[Segment] =
    Iterable.empty

  override def collapseAllSmallSegments(batch: Int): Try[Int] =
    Success(0)

  override def existsOnDisk =
    false

  override def levelSize = 0

  override def segmentCountAndLevelSize =
    (0, 0)

  override def putMap(map: Map[Slice[Byte], (ValueType, Option[Slice[Byte]])]) =
    Success()

  override def head =
    Success(None)

  override def last =
    Success(None)

  override def get(key: Slice[Byte]) =
    Success(None)

  override def lower(key: Slice[Byte]) =
    Success(None)

  override def higher(key: Slice[Byte]) =
    Success(None)

  override def isEmpty: Boolean =
    true

  override def isTrash: Boolean = true

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    Iterable.empty

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    Iterable.empty

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    Iterable.empty

  override def sizeOfSegments: Long =
    0

  override def releaseLocks: Try[Unit] =
    Success()

  override def close: Try[Unit] =
    Success()

  override def meter: LevelMeter =
    LevelMeter(0, 0)

  override def meterFor(levelNumber: Int): Option[LevelMeter] =
    None

  override def mightContain(key: Slice[Byte]): Try[Boolean] =
    Success(false)
}
