/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.io

import swaydb.core.data.KeyValue
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.io.{SegmentReadIO, SegmentWriteIO}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Actor, DefActor}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Responsible for performing write and read IO during compaction.
 *
 * This actor is created per compaction cycle and accumulates Segments created.
 * On error those segments are deleted when the Actor is terminated.
 */
case object CompactionIO {

  type Actor = DefActor[CompactionIO, Unit]

  sealed trait State {
    def segments: ConcurrentHashMap[Segment, Unit]

    def segmentsAsScala(): Iterator[Segment] =
      segments.keys().asScala
  }

  object State {
    //Stores the Segments persisted by this Actor.
    case class Success(segments: ConcurrentHashMap[Segment, Unit]) extends State
    //Stores failure cause and the Segments that should be deleted on termination.
    case class Failed(cause: Throwable, segments: ConcurrentHashMap[Segment, Unit]) extends State
  }

  def create()(implicit ec: ExecutionContext): CompactionIO.Actor =
    Actor.define[CompactionIO, Unit](
      name = CompactionIO.productPrefix,
      state = (),
      init = _ => new CompactionIO(State.Success(new ConcurrentHashMap()))
    ).onPreTerminate {
      (instance, _, _) =>
        instance.state match {
          case State.Failed(_, segments) =>
            segments forEach {
              (segment: Segment, _: Unit) =>
                segment.delete
            }

          case State.Success(_) =>
        }
    }.start()
}

class CompactionIO(@volatile private var state: CompactionIO.State) {

  def iterator[S <: Segment](segment: S, inOneSeek: Boolean): Future[Iterator[KeyValue]] =
    Future.successful(segment.iterator(inOneSeek))

  def segments(): Iterator[Segment] =
    state.segmentsAsScala()

  def isSuccess(): Boolean =
    this.state match {
      case CompactionIO.State.Success(_) =>
        true

      case CompactionIO.State.Failed(_, _) =>
        false
    }

  def isFailed(): Boolean =
    !isSuccess()

  def persist[T <: TransientSegment, S <: Segment](pathsDistributor: PathsDistributor,
                                                   segmentRefCacheLife: SegmentRefCacheLife,
                                                   mmap: MMAP.Segment,
                                                   transient: Iterable[T])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                           functionStore: FunctionStore,
                                                                           fileSweeper: FileSweeper,
                                                                           bufferCleaner: ByteBufferSweeperActor,
                                                                           keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                           blockCacheSweeper: Option[MemorySweeper.Block],
                                                                           segmentReadIO: SegmentReadIO,
                                                                           idGenerator: IDGenerator,
                                                                           forceSaveApplier: ForceSaveApplier,
                                                                           segmentWriteIO: SegmentWriteIO[T, S]): Future[Iterable[S]] =
    state match {
      case CompactionIO.State.Success(segments) =>
        //if the state is then persist the segment
        segmentWriteIO.persistTransient(
          pathsDistributor = pathsDistributor,
          segmentRefCacheLife = segmentRefCacheLife,
          mmap = mmap,
          transient = transient
        ) onRightSideEffect {
          success =>
            success foreach {
              segment =>
                segments.put(segment, ())
            }
        } onLeftSideEffect {
          error =>
            state = CompactionIO.State.Failed(error.exception, segments)
        } toFuture

      case CompactionIO.State.Failed(cause, _) =>
        //if the state is failure then ignore creating new Segments
        //and return existing failure
        Future.failed(cause)
    }
}
