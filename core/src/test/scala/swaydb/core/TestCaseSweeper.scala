/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.TestSweeper._
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.MemorySweeper
import swaydb.core.io.file.{DBFile, Effect, ForceSaveApplier}
import swaydb.core.level.LevelRef
import swaydb.core.map.Maps
import swaydb.core.map.counter.CounterMap
import swaydb.core.segment.Segment
import swaydb.core.segment.block.BlockCache
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.data.RunThis._
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.{ActorRef, DefActor, Bag, Glass, Scheduler}

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
      blockCaches = ListBuffer(Cache.noIO[Unit, Option[BlockCache.State]](true, true, None)((_, _) => createBlockCacheRandom())),
      allMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.All]](true, true, None)((_, _) => createMemorySweeperMax())),
      keyValueMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom())),
      blockMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Block]](true, true, None)((_, _) => createMemoryBlockSweeper())),
      cacheMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Cache]](true, true, None)((_, _) => createRandomCacheSweeper())),
      schedulers = ListBuffer(Cache.noIO[Unit, Scheduler](true, true, None)((_, _) => Scheduler()(DefaultExecutionContext.sweeperEC))),
      levels = ListBuffer.empty,
      segments = ListBuffer.empty,
      mapFiles = ListBuffer.empty,
      maps = ListBuffer.empty,
      dbFiles = ListBuffer.empty,
      paths = ListBuffer.empty,
      actors = ListBuffer.empty,
      counters = ListBuffer.empty,
      actorWires = ListBuffer.empty,
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
    sweeper.actorWires.foreach(_.terminateAndClear[Glass]())

    sweeper.schedulers.foreach(_.get().foreach(_.terminate()))

    //CLOSE - close everything first so that Actors sweepers get populated with Clean messages
    sweeper.dbFiles.foreach(_.close())
    sweeper.mapFiles.foreach(_.close())
    sweeper.maps.foreach(_.delete().get)
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

  implicit class TestMapFilesSweeperImplicits[M <: swaydb.core.map.Map[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestCaseSweeper): M =
      sweeper sweepMap map
  }

  implicit class TestMapsSweeperImplicits[M <: swaydb.core.map.Maps[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestCaseSweeper): M =
      sweeper sweepMaps map
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

  implicit class CountersSweeperImplicits[C <: CounterMap](counter: C) {
    def sweep()(implicit sweeper: TestCaseSweeper): C =
      sweeper sweepCounter counter
  }

  implicit class ActorWiresSweeperImplicits[T, S](actor: DefActor[T, S]) {
    def sweep()(implicit sweeper: TestCaseSweeper): DefActor[T, S] =
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
                      private val blockCaches: ListBuffer[CacheNoIO[Unit, Option[BlockCache.State]]],
                      private val allMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.All]]],
                      private val keyValueMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.KeyValue]]],
                      private val blockMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Block]]],
                      private val cacheMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Cache]]],
                      private val schedulers: ListBuffer[CacheNoIO[Unit, Scheduler]],
                      private val levels: ListBuffer[LevelRef],
                      private val segments: ListBuffer[Segment],
                      private val mapFiles: ListBuffer[map.Map[_, _, _]],
                      private val maps: ListBuffer[Maps[_, _, _]],
                      private val dbFiles: ListBuffer[DBFile],
                      private val paths: ListBuffer[Path],
                      private val actors: ListBuffer[ActorRef[_, _]],
                      private val actorWires: ListBuffer[DefActor[_, _]],
                      private val counters: ListBuffer[CounterMap],
                      private val functions: ListBuffer[() => Unit]) {


  implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On
  implicit lazy val fileSweeper: FileSweeper.On = fileSweepers.head.value(())
  implicit lazy val cleaner: ByteBufferSweeperActor = cleaners.head.value(())
  implicit lazy val blockCache: Option[BlockCache.State] = blockCaches.head.value(())

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

  def sweepMap[M <: swaydb.core.map.Map[_, _, _]](map: M): M = {
    mapFiles += map
    map
  }

  def sweepMaps[M <: Maps[_, _, _]](maps: M): M = {
    this.maps += maps
    maps
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

  def sweepActor[T, S](actor: ActorRef[T, S]): ActorRef[T, S] = {
    actors += actor
    actor
  }

  def sweepWireActor[T, S](actor: DefActor[T, S]): DefActor[T, S] = {
    actorWires += actor
    actor
  }

  def sweepCounter[C <: CounterMap](counter: C): C = {
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
