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

package swaydb.core.compaction.io

import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.{FunctionStore, Segment}
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.KeyValue
import swaydb.core.segment.io.{SegmentReadIO, SegmentWriteIO}
import swaydb.core.util.IDGenerator
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
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

  type Actor = DefActor[CompactionIO]

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
    Actor.define[CompactionIO](
      name = CompactionIO.productPrefix,
      init = _ => new CompactionIO(State.Success(new ConcurrentHashMap()))
    ).onPreTerminate {
      (instance, _) =>
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
