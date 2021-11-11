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
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.TestSweeper._
import swaydb.core.io.file.{DBFile, ForceSaveApplier}
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.log.counter.CounterLog
import swaydb.core.log.{Log, Logs}
import swaydb.core.segment.Segment
import swaydb.core.segment.block.BlockCache
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.effect.Effect
import swaydb.testkit.RunThis._
import swaydb.{ActorRef, Bag, DefActor, Glass, Scheduler}

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
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

object TestCaseSweeper extends LazyLogging {

  def apply(): TestCaseSweeper =
    new TestCaseSweeper(
      fileSweepers = ListBuffer(Cache.noIO[Unit, FileSweeper.On](true, true, None)((_, _) => createFileSweeper())),
      cleaners = ListBuffer(Cache.noIO[Unit, ByteBufferSweeperActor](true, true, None)((_, _) => createBufferCleaner())),
      compactionIOActors = ListBuffer(Cache.noIO[Unit, CompactionIO.Actor](true, true, None)((_, _) => CompactionIO.create()(TestExecutionContext.executionContext))),
      blockCaches = ListBuffer(Cache.noIO[Unit, Option[BlockCache.State]](true, true, None)((_, _) => createBlockCacheRandom())),
      allMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.All]](true, true, None)((_, _) => createMemorySweeperMax())),
      keyValueMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom())),
      blockMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Block]](true, true, None)((_, _) => createMemoryBlockSweeper())),
      cacheMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Cache]](true, true, None)((_, _) => createRandomCacheSweeper())),
      schedulers = ListBuffer(Cache.noIO[Unit, Scheduler](true, true, None)((_, _) => Scheduler()(DefaultExecutionContext.sweeperEC))),
      levels = ListBuffer.empty,
      segments = ListBuffer.empty,
      mapFiles = ListBuffer.empty,
      logs = ListBuffer.empty,
      dbFiles = ListBuffer.empty,
      paths = ListBuffer.empty,
      actors = ListBuffer.empty,
      counters = ListBuffer.empty,
      defActors = ListBuffer.empty,
      functions = ListBuffer.empty
    )

  def deleteParentPath(path: Path) = {
    val parentPath = path.getParent
    //also delete parent folder of Segment. TestSegments are created with a parent folder.
    if (Effect.exists(parentPath) && Try(parentPath.getFileName.toString.toInt).isSuccess)
      Effect.walkDelete(parentPath)
  }

  private def terminate(sweeper: TestCaseSweeper): Unit = {
    logger.info(s"Terminating ${classOf[TestCaseSweeper].getSimpleName}")

    sweeper.actors.foreach(_.terminateAndClear[Glass]())
    sweeper.defActors.foreach(_.terminateAndClear[Glass]())

    sweeper.schedulers.foreach(_.get().foreach(_.terminate()))

    //CLOSE - close everything first so that Actors sweepers get populated with Clean messages
    sweeper.dbFiles.foreach(_.close())
    sweeper.mapFiles.foreach(_.close())
    sweeper.logs.foreach(_.delete().get)
    sweeper.segments.foreach(_.close)
    sweeper.levels.foreach(_.close[Glass]())
    sweeper.counters.foreach(_.close)

    //TERMINATE - terminate all initialised actors
    sweeper.keyValueMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.allMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.blockMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.cacheMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.fileSweepers.foreach(_.get().foreach(sweeper => FileSweeper.close()(sweeper, Bag.glass)))
    sweeper.cleaners.foreach(_.get().foreach(cleaner => ByteBufferSweeper.close()(cleaner, Bag.glass)))
    sweeper.blockCaches.foreach(_.get().foreach(BlockCache.close))
    sweeper.compactionIOActors.foreach(_.get().foreach(_.terminate[Glass]()))

    //DELETE - delete after closing Levels.
    sweeper.levels.foreach(_.delete[Glass]())

    sweeper.segments.foreach {
      segment =>
        if (segment.existsOnDisk)
          segment.delete
        else
          segment.close //the test itself might've delete this file so submit close just in-case.

        //eventual because segment.delete goes to an actor which might eventually get resolved.
        eventual(20.seconds)(deleteParentPath(segment.path))
    }

    sweeper.mapFiles.foreach {
      map =>
        if (map.pathOption.exists(Effect.exists))
          map.pathOption.foreach(Effect.walkDelete)
        map.pathOption.foreach(deleteParentPath)
    }

    sweeper.dbFiles.foreach(_.delete())
    sweeper.functions.foreach(_ ())

    sweeper.paths.foreach {
      path =>
        eventual(10.seconds)(Effect.walkDelete(path))
    }
  }

  private def receiveAll(sweeper: TestCaseSweeper): Unit = {
    sweeper.keyValueMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.allMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.blockMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.cacheMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.fileSweepers.foreach(_.get().foreach {
      actor =>
        actor.closer.receiveAllForce[Glass, Unit](_ => ())
        actor.deleter.receiveAllForce[Glass, Unit](_ => ())
    })
    sweeper.cleaners.foreach(_.get().foreach(_.actor.receiveAllForce[Glass, Unit](_ => ())))
    sweeper.blockCaches.foreach(_.get().foreach(_.foreach(_.sweeper.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
  }

  def apply[T](code: TestCaseSweeper => T): T = {
    val sweeper = TestCaseSweeper()
    val result = code(sweeper)
    terminate(sweeper)
    result
  }

  implicit class TestLevelLevelSweeperImplicits[L <: LevelRef](level: L) {
    def sweep()(implicit sweeper: TestCaseSweeper): L =
      sweeper sweepLevel level
  }

  implicit class TestLevelSegmentSweeperImplicits[L <: Segment](segment: L) {
    def sweep()(implicit sweeper: TestCaseSweeper): L =
      sweeper sweepSegment segment
  }

  implicit class TestMapFilesSweeperImplicits[M <: Log[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestCaseSweeper): M =
      sweeper sweepLog map
  }

  implicit class TestMapsSweeperImplicits[M <: Logs[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestCaseSweeper): M =
      sweeper sweepLogs map
  }

  implicit class TestLevelPathSweeperImplicits(path: Path) {
    def sweep()(implicit sweeper: TestCaseSweeper): Path =
      sweeper sweepPath path
  }

  implicit class DBFileSweeperImplicits(dbFile: DBFile) {
    def sweep()(implicit sweeper: TestCaseSweeper): DBFile =
      sweeper sweepDBFiles dbFile
  }

  implicit class KeyValueMemorySweeperImplicits(keyValue: MemorySweeper.KeyValue) {
    def sweep()(implicit sweeper: TestCaseSweeper): MemorySweeper.KeyValue =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockSweeperImplicits(keyValue: MemorySweeper.Block) {
    def sweep()(implicit sweeper: TestCaseSweeper): MemorySweeper.Block =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockCacheStateSweeperImplicits(state: BlockCache.State) {
    def sweep()(implicit sweeper: TestCaseSweeper): BlockCache.State =
      sweeper sweepBlockCacheState state
  }

  implicit class AllMemorySweeperImplicits(actor: MemorySweeper.All) {
    def sweep()(implicit sweeper: TestCaseSweeper): MemorySweeper.All =
      sweeper sweepMemorySweeper actor
  }

  implicit class BufferCleanerSweeperImplicits(actor: ByteBufferSweeperActor) {
    def sweep()(implicit sweeper: TestCaseSweeper): ByteBufferSweeperActor =
      sweeper sweepBufferCleaner actor
  }

  implicit class CompactionIOActorImplicits(actor: CompactionIO.Actor) {
    def sweep()(implicit sweeper: TestCaseSweeper): CompactionIO.Actor =
      sweeper sweepCompactionIOActors actor
  }

  implicit class FileCleanerSweeperImplicits(actor: FileSweeper.On) {
    def sweep()(implicit sweeper: TestCaseSweeper): FileSweeper.On =
      sweeper sweepFileSweeper actor
  }

  implicit class SchedulerSweeperImplicits(scheduler: Scheduler) {
    def sweep()(implicit sweeper: TestCaseSweeper): Scheduler =
      sweeper sweepScheduler scheduler
  }

  implicit class ActorsSweeperImplicits[T, S](actor: ActorRef[T, S]) {
    def sweep()(implicit sweeper: TestCaseSweeper): ActorRef[T, S] =
      sweeper sweepActor actor
  }

  implicit class CountersSweeperImplicits[C <: CounterLog](counter: C) {
    def sweep()(implicit sweeper: TestCaseSweeper): C =
      sweeper sweepCounter counter
  }

  implicit class ActorWiresSweeperImplicits[T](actor: DefActor[T]) {
    def sweep()(implicit sweeper: TestCaseSweeper): DefActor[T] =
      sweeper sweepWireActor actor
  }

  implicit class FunctionSweeperImplicits[BAG[_], T](sweepable: T) {
    def sweep(f: T => Unit)(implicit sweeper: TestCaseSweeper): T =
      sweeper.sweepItem(sweepable, f)
  }
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

class TestCaseSweeper(private val fileSweepers: ListBuffer[CacheNoIO[Unit, FileSweeper.On]],
                      private val cleaners: ListBuffer[CacheNoIO[Unit, ByteBufferSweeperActor]],
                      private val compactionIOActors: ListBuffer[CacheNoIO[Unit, CompactionIO.Actor]],
                      private val blockCaches: ListBuffer[CacheNoIO[Unit, Option[BlockCache.State]]],
                      private val allMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.All]]],
                      private val keyValueMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.KeyValue]]],
                      private val blockMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Block]]],
                      private val cacheMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Cache]]],
                      private val schedulers: ListBuffer[CacheNoIO[Unit, Scheduler]],
                      private val levels: ListBuffer[LevelRef],
                      private val segments: ListBuffer[Segment],
                      private val mapFiles: ListBuffer[Log[_, _, _]],
                      private val logs: ListBuffer[Logs[_, _, _]],
                      private val dbFiles: ListBuffer[DBFile],
                      private val paths: ListBuffer[Path],
                      private val actors: ListBuffer[ActorRef[_, _]],
                      private val defActors: ListBuffer[DefActor[_]],
                      private val counters: ListBuffer[CounterLog],
                      private val functions: ListBuffer[() => Unit]) {


  implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On
  implicit lazy val fileSweeper: FileSweeper.On = fileSweepers.head.value(())
  implicit lazy val cleaner: ByteBufferSweeperActor = cleaners.head.value(())
  implicit lazy val blockCache: Option[BlockCache.State] = blockCaches.head.value(())
  implicit lazy val compactionIOActor: CompactionIO.Actor = compactionIOActors.head.value(())

  //MemorySweeper.All can also be set which means other sweepers will search for dedicated sweepers first and
  //if not found then the head from allMemorySweeper is fetched.
  private def allMemorySweeper(): Option[MemorySweeper.All] = allMemorySweepers.head.value(())
  //if dedicated sweepers are not supplied then fetch from sweepers of type All (which enables all sweeper types).
  implicit lazy val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = keyValueMemorySweepers.head.value(()) orElse allMemorySweeper()
  implicit lazy val blockSweeperCache: Option[MemorySweeper.Block] = blockMemorySweepers.head.value(()) orElse allMemorySweeper()
  implicit lazy val cacheMemorySweeper: Option[MemorySweeper.Cache] = cacheMemorySweepers.head.value(()) orElse allMemorySweeper()

  implicit lazy val scheduler = schedulers.head.value(())

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

  def sweepPath(path: Path): Path = {
    paths += path
    path
  }

  def sweepDBFiles(file: DBFile): DBFile = {
    dbFiles += file
    file
  }

  private def removeReplaceOptionalCache[I, O](sweepers: ListBuffer[CacheNoIO[I, Option[O]]], replace: O): O = {
    if (sweepers.lastOption.exists(_.get().isEmpty))
      sweepers.remove(0)

    val cache = Cache.noIO[I, Option[O]](true, true, Some(Some(replace)))((_, _) => Some(replace))
    sweepers += cache
    replace
  }

  private def removeReplaceCache[I, O](sweepers: ListBuffer[CacheNoIO[I, O]], replace: O): O = {
    if (sweepers.lastOption.exists(_.get().isEmpty))
      sweepers.remove(0)

    val cache = Cache.noIO[I, O](true, true, Some(replace))((_, _) => replace)
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

  def sweepBlockCacheState(state: BlockCache.State): BlockCache.State =
    removeReplaceOptionalCache(blockCaches, state)

  def sweepBufferCleaner(bufferCleaner: ByteBufferSweeperActor): ByteBufferSweeperActor =
    removeReplaceCache(cleaners, bufferCleaner)

  def sweepFileSweeper(sweeper: FileSweeper.On): FileSweeper.On =
    removeReplaceCache(fileSweepers, sweeper)

  def sweepScheduler(schedule: Scheduler): Scheduler =
    removeReplaceCache(schedulers, schedule)

  def sweepCompactionIOActors(actor: CompactionIO.Actor): CompactionIO.Actor =
    removeReplaceCache(compactionIOActors, actor)

  def sweepActor[T, S](actor: ActorRef[T, S]): ActorRef[T, S] = {
    actors += actor
    actor
  }

  def sweepWireActor[T](actor: DefActor[T]): DefActor[T] = {
    defActors += actor
    actor
  }

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
  def terminateNow(): Unit =
    TestCaseSweeper.terminate(this)

  def receiveAll(): Unit =
    TestCaseSweeper.receiveAll(this)
}
