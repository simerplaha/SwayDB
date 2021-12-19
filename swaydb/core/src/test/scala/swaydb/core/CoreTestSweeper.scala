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

package swaydb.core

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Bag, Glass, TestExecutionContext}
import swaydb.actor.ActorTestSweeper
import swaydb.core.cache.{Cache, CacheUnsafe}
import swaydb.core.file.{CoreFile, ForceSaveApplier}
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeperTestKit._
import swaydb.core.level.LevelRef
import swaydb.core.log.{Log, Logs}
import swaydb.core.log.counter.CounterLog
import swaydb.core.segment.{CoreFunctionStore, Segment, TestCoreFunctionStore}
import swaydb.core.segment.block.{BlockCache, BlockCacheState}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.cache.sweeper.MemorySweeperTestKit._
import swaydb.core.segment.io.SegmentCompactionIO
import swaydb.core.CoreTestSweeper.deleteParentPath
import swaydb.core.segment.distributor.PathDistributor
import swaydb.effect.{Effect, EffectTestSweeper}
import swaydb.testkit.RunThis._

import java.nio.file.Path
import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters.IterableIsParallelizable
import scala.concurrent.duration.DurationInt
import scala.util.Try

/**
 * Manages cleaning levels, segments, maps etc for Levels. Initialises Actors lazily as required and
 * destroys them after the test is complete.
 *
 * {{{
 *   TestLevelSweeper {
 *      implicit sweeper =>
 *          // test code here amd all levels, segments creates
 *          // here will get deleted after the test is complete.
 *   }
 * }}}
 */


object CoreTestSweeper extends LazyLogging {

  private def deleteParentPath(path: Path) = {
    val parentPath = path.getParent
    //also delete parent folder of Segment. TestSegments are created with a parent folder.
    if (Effect.exists(parentPath) && Try(parentPath.getFileName.toString.toInt).isSuccess)
      Effect.walkDelete(parentPath)
  }

  def repeat(times: Int, log: Boolean = true)(code: CoreTestSweeper => Unit): Unit = {
    import swaydb.testkit.RunThis._
    runThis(times, log)(CoreTestSweeper[Unit](code))
  }

  def foreachRepeat[S](times: Int, states: Iterable[S])(code: (CoreTestSweeper, S) => Unit): Unit =
    runThis(times, log = true) {
      foreach(states)(code)
    }

  def foreach[S](states: Iterable[S])(code: (CoreTestSweeper, S) => Unit): Unit =
    states foreach {
      state =>
        CoreTestSweeper {
          sweeper =>
            code(sweeper, state)
        }
    }

  def foreachParallel[S](states: Iterable[S])(code: (CoreTestSweeper, S) => Unit): Unit =
    states.par foreach {
      state =>
        CoreTestSweeper {
          sweeper =>
            code(sweeper, state)
        }
    }

  def apply[T](code: CoreTestSweeper => T): T = {
    val sweeper = new CoreTestSweeper {}
    val result = code(sweeper)
    sweeper.terminate()
    result
  }

  implicit class TestLevelLevelSweeperImplicits[L <: LevelRef](level: L) {
    def sweep()(implicit sweeper: CoreTestSweeper): L =
      sweeper sweepLevel level
  }

  implicit class TestLevelSegmentSweeperImplicits[L <: Segment](segment: L) {
    def sweep()(implicit sweeper: CoreTestSweeper): L =
      sweeper sweepSegment segment
  }

  implicit class TestMapFilesSweeperImplicits[M <: Log[_, _, _]](map: M) {
    def sweep()(implicit sweeper: CoreTestSweeper): M =
      sweeper sweepLog map
  }

  implicit class TestMapsSweeperImplicits[M <: Logs[_, _, _]](map: M) {
    def sweep()(implicit sweeper: CoreTestSweeper): M =
      sweeper sweepLogs map
  }

  implicit class CoreFileSweeperImplicits(coreFile: CoreFile) {
    def sweep()(implicit sweeper: CoreTestSweeper): CoreFile =
      sweeper sweepCoreFiles coreFile
  }

  implicit class KeyValueMemorySweeperImplicits(keyValue: MemorySweeper.KeyValue) {
    def sweep()(implicit sweeper: CoreTestSweeper): MemorySweeper.KeyValue =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockSweeperImplicits(keyValue: MemorySweeper.Block) {
    def sweep()(implicit sweeper: CoreTestSweeper): MemorySweeper.Block =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockCacheStateSweeperImplicits(state: BlockCacheState) {
    def sweep()(implicit sweeper: CoreTestSweeper): BlockCacheState =
      sweeper sweepBlockCacheState state
  }

  implicit class AllMemorySweeperImplicits(actor: MemorySweeper.All) {
    def sweep()(implicit sweeper: CoreTestSweeper): MemorySweeper.All =
      sweeper sweepMemorySweeper actor
  }

  implicit class BufferCleanerSweeperImplicits(actor: ByteBufferSweeperActor) {
    def sweep()(implicit sweeper: CoreTestSweeper): ByteBufferSweeperActor =
      sweeper sweepBufferCleaner actor
  }

  implicit class CompactionIOActorImplicits(actor: SegmentCompactionIO.Actor) {
    def sweep()(implicit sweeper: CoreTestSweeper): SegmentCompactionIO.Actor =
      sweeper sweepCompactionIOActors actor
  }

  implicit class FileCleanerSweeperImplicits(actor: FileSweeper.On) {
    def sweep()(implicit sweeper: CoreTestSweeper): FileSweeper.On =
      sweeper sweepFileSweeper actor
  }

  implicit class CountersSweeperImplicits[C <: CounterLog](counter: C) {
    def sweep()(implicit sweeper: CoreTestSweeper): C =
      sweeper sweepCounter counter
  }

//  implicit class FunctionSweeperImplicits[BAG[_], T](sweepable: T) {
//    def sweep(f: T => Unit)(implicit sweeper: CoreTestSweeper): T =
//      sweeper.sweepItem(sweepable, f)
//  }
}


/**
 * Manages cleaning levels, segments, maps etc for Levels. Initialises Actors lazily as required and
 * destroys them after the test is complete.
 *
 * {{{
 *   TestLevelSweeper {
 *      implicit sweeper =>
 *          // test code here and all levels, segments creates
 *          // here will get deleted after the test is complete.
 *   }
 * }}}
 */

trait CoreTestSweeper extends ActorTestSweeper with EffectTestSweeper with LazyLogging {

  @BeanProperty protected var deleteFiles: Boolean = true

  private val fileSweepers: ListBuffer[CacheUnsafe[Unit, FileSweeper.On]] = ListBuffer(Cache.unsafe[Unit, FileSweeper.On](true, true, None)((_, _) => createFileSweeper()))
  private val cleaners: ListBuffer[CacheUnsafe[Unit, ByteBufferSweeperActor]] = ListBuffer(Cache.unsafe[Unit, ByteBufferSweeperActor](true, true, None)((_, _) => createBufferCleaner()))
  private val compactionIOActors: ListBuffer[CacheUnsafe[Unit, SegmentCompactionIO.Actor]] = ListBuffer(Cache.unsafe[Unit, SegmentCompactionIO.Actor](true, true, None)((_, _) => SegmentCompactionIO.create()(TestExecutionContext.executionContext)))
  private val blockCaches: ListBuffer[CacheUnsafe[Unit, Option[BlockCacheState]]] = ListBuffer(Cache.unsafe[Unit, Option[BlockCacheState]](true, true, None)((_, _) => createBlockCacheRandom()))
  private val allMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.All]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.All]](true, true, None)((_, _) => createMemorySweeperMax()))
  private val keyValueMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.KeyValue]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom()))
  private val blockMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.Block]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.Block]](true, true, None)((_, _) => createMemoryBlockSweeper()))
  private val cacheMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.Cache]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.Cache]](true, true, None)((_, _) => createRandomCacheSweeper()))
  private val levels: ListBuffer[LevelRef] = ListBuffer.empty
  private val segments: ListBuffer[Segment] = ListBuffer.empty
  private val mapFiles: ListBuffer[Log[_, _, _]] = ListBuffer.empty
  private val logs: ListBuffer[Logs[_, _, _]] = ListBuffer.empty
  private val coreFiles: ListBuffer[CoreFile] = ListBuffer.empty
  private val counters: ListBuffer[CounterLog] = ListBuffer.empty
  private val functions: ListBuffer[() => Unit] = ListBuffer.empty

  implicit lazy val pathDistributor: PathDistributor = {
    //      def persistentSubDirectories(): Seq[Path] =
    //        (0 until subDirectories) map {
    //          intDir =>
    //            sweepPath(Effect.createDirectoriesIfAbsent(testPath.resolve(intDir.toString)))
    //        }
    //
    //      val addDirectories =
    //        Seq(persistTestPath()) ++ persistentSubDirectories() map {
    //          path =>
    //            Dir(path, 1)
    //        }
    //
    //      PathDistributor(addDirectories, () => Seq.empty)
    ???
  }

  implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On
  implicit lazy val fileSweeper: FileSweeper.On = fileSweepers.head.getOrFetch(())
  implicit lazy val cleaner: ByteBufferSweeperActor = cleaners.head.getOrFetch(())
  implicit lazy val blockCache: Option[BlockCacheState] = blockCaches.head.getOrFetch(())
  implicit lazy val compactionIOActor: SegmentCompactionIO.Actor = compactionIOActors.head.getOrFetch(())

  //MemorySweeper.All can also be set which means other sweepers will search for dedicated sweepers first and
  //if not found then the head from allMemorySweeper is fetched.
  private def allMemorySweeper(): Option[MemorySweeper.All] = allMemorySweepers.head.getOrFetch(())
  //if dedicated sweepers are not supplied then fetch from sweepers of type All (which enables all sweeper types).
  implicit lazy val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = keyValueMemorySweepers.head.getOrFetch(()) orElse allMemorySweeper()
  implicit lazy val blockSweeperCache: Option[MemorySweeper.Block] = blockMemorySweepers.head.getOrFetch(()) orElse allMemorySweeper()
  implicit lazy val cacheMemorySweeper: Option[MemorySweeper.Cache] = cacheMemorySweepers.head.getOrFetch(()) orElse allMemorySweeper()

  lazy implicit val testCoreFunctionStore: TestCoreFunctionStore = TestCoreFunctionStore()
  lazy implicit val coreFunctionStore: CoreFunctionStore = testCoreFunctionStore.store

  def sweepLevel[T <: LevelRef](levelRef: T): T = {
    levels += levelRef
    levelRef
  }

  def sweepSegment[S <: Segment](segment: S): S = {
    segments += segment
    segment
  }

  def sweepLog[M <: Log[_, _, _]](map: M): M = {
    mapFiles += map
    map
  }

  def sweepLogs[M <: Logs[_, _, _]](logs: M): M = {
    this.logs += logs
    logs
  }

  def sweepCoreFiles(file: CoreFile): CoreFile = {
    coreFiles += file
    file
  }

  private def removeReplaceOptionalCache[I, O](sweepers: ListBuffer[CacheUnsafe[I, Option[O]]], replace: O): O = {
    if (sweepers.lastOption.exists(_.get().isEmpty))
      sweepers.remove(0)

    val cache = Cache.unsafe[I, Option[O]](true, true, Some(Some(replace)))((_, _) => Some(replace))
    sweepers += cache
    replace
  }

  def sweepMemorySweeper(sweeper: MemorySweeper.Cache): MemorySweeper.Cache =
    removeReplaceOptionalCache(cacheMemorySweepers, sweeper)

  def sweepMemorySweeper(sweeper: MemorySweeper.KeyValue): MemorySweeper.KeyValue =
    removeReplaceOptionalCache(keyValueMemorySweepers, sweeper)

  def sweepMemorySweeper(sweeper: MemorySweeper.Block): MemorySweeper.Block =
    removeReplaceOptionalCache(blockMemorySweepers, sweeper)

  def sweepMemorySweeper(sweeper: MemorySweeper.All): MemorySweeper.All =
    removeReplaceOptionalCache(allMemorySweepers, sweeper)

  def sweepBlockCacheState(state: BlockCacheState): BlockCacheState =
    removeReplaceOptionalCache(blockCaches, state)

  def sweepBufferCleaner(bufferCleaner: ByteBufferSweeperActor): ByteBufferSweeperActor =
    removeReplaceCache(cleaners, bufferCleaner)

  def sweepFileSweeper(sweeper: FileSweeper.On): FileSweeper.On =
    removeReplaceCache(fileSweepers, sweeper)

  def sweepCompactionIOActors(actor: SegmentCompactionIO.Actor): SegmentCompactionIO.Actor =
    removeReplaceCache(compactionIOActors, actor)

  def sweepCounter[C <: CounterLog](counter: C): C = {
    counters += counter
    counter
  }

  def sweepItem[T](item: T, sweepable: T => Unit): T = {
    functions += (() => sweepable(item))
    item
  }

  /**
   * Terminates all sweepers immediately.
   */
  def terminate(): Unit = {
    logger.info(s"Terminating ${classOf[CoreTestSweeper].getSimpleName}")

    super.terminateAllActors()

    //CLOSE - close everything first so that Actors sweepers get populated with Clean messages
    coreFiles.foreach(_.close())
    mapFiles.foreach(_.close())
    logs.foreach(_.delete().get)
    segments.foreach(_.close())
    levels.foreach(_.close[Glass]())
    counters.foreach(_.close())

    //TERMINATE - terminate all initialised actors
    keyValueMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    allMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    blockMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    cacheMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    fileSweepers.foreach(_.get().foreach(sweeper => FileSweeper.close()(sweeper, Bag.glass)))
    cleaners.foreach(_.get().foreach(cleaner => ByteBufferSweeper.close()(cleaner, Bag.glass)))
    blockCaches.foreach(_.get().foreach(BlockCache.close))
    compactionIOActors.foreach(_.get().foreach(_.terminate[Glass]()))

    //DELETE - delete after closing Levels.
    if (deleteFiles) levels.foreach(_.delete[Glass]())

    segments.foreach {
      segment =>
        if (deleteFiles && segment.existsOnDisk())
          segment.delete()
        else
          segment.close() //the test itself might've delete this file so submit close just in-case.

        //eventual because segment.delete() goes to an actor which might eventually get resolved.
        if (deleteFiles)
          eventual(20.seconds)(deleteParentPath(segment.path))
    }

    if (deleteFiles) {
      mapFiles.foreach {
        map =>
          if (map.pathOption.exists(Effect.exists))
            map.pathOption.foreach(Effect.walkDelete)
          map.pathOption.foreach(deleteParentPath)
      }

      coreFiles.foreach(_.delete())
      functions.foreach(_ ())

      super.deleteAllPaths()
      eventual(10.seconds)(testDirectory)
    }
  }

  def receiveAll(): Unit = {
    keyValueMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    allMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    blockMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    cacheMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    fileSweepers.foreach(_.get().foreach {
      actor =>
        actor.closer.receiveAllForce[Glass, Unit](_ => ())
        actor.deleter.receiveAllForce[Glass, Unit](_ => ())
    })
    cleaners.foreach(_.get().foreach(_.actor().receiveAllForce[Glass, Unit](_ => ())))
    blockCaches.foreach(_.get().foreach(_.foreach(_.sweeper.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
  }
}
