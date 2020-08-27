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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core

import java.nio.file.{NoSuchFileException, Path}

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.TestSweeper._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.core.io.file.{BlockCache, DBFile, Effect, ForceSaveApplier}
import swaydb.core.level.LevelRef
import swaydb.core.map.Maps
import swaydb.core.segment.Segment
import swaydb.data.RunThis._
import swaydb.data.Sweepable
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.{ActorRef, ActorWire, Bag, Scheduler}

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
      fileSweepers = ListBuffer(Cache.noIO[Unit, FileSweeperActor](true, true, None)((_, _) => createFileSweeper())),
      cleaners = ListBuffer(Cache.noIO[Unit, ByteBufferSweeperActor](true, true, None)((_, _) => createBufferCleaner())),
      blockCaches = ListBuffer(Cache.noIO[Unit, Option[BlockCache.State]](true, true, None)((_, _) => createBlockCacheRandom())),
      allMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.All]](true, true, None)((_, _) => createMemorySweeperMax())),
      keyValueMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom())),
      blockMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Block]](true, true, None)((_, _) => createMemoryBlockSweeper())),
      cacheMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.Cache]](true, true, None)((_, _) => createRandomCacheSweeper())),
      schedulers = ListBuffer(Cache.noIO[Unit, Scheduler](true, true, None)((_, _) => Scheduler()(TestExecutionContext.executionContext))),
      levels = ListBuffer.empty,
      segments = ListBuffer.empty,
      mapFiles = ListBuffer.empty,
      maps = ListBuffer.empty,
      dbFiles = ListBuffer.empty,
      paths = ListBuffer.empty,
      actors = ListBuffer.empty,
      actorWires = ListBuffer.empty,
      sweepables = ListBuffer.empty
    )

  def deleteParentPath(path: Path) = {
    val parentPath = path.getParent
    //also delete parent folder of Segment. TestSegments are created with a parent folder.
    if (Effect.exists(parentPath) && Try(parentPath.getFileName.toString.toInt).isSuccess)
      Effect.walkDelete(parentPath)
  }

  private def terminate(sweeper: TestCaseSweeper): Unit = {
    logger.info(s"Terminating ${classOf[TestCaseSweeper].getSimpleName}")

    implicit val ec = TestExecutionContext.executionContext

    sweeper.actors.foreach(_.terminateAndClear[Bag.Less]())
    sweeper.actorWires.foreach(_.terminateAndClear[Bag.Less]())

    sweeper.schedulers.foreach(_.get().foreach(_.terminate()))

    //CLOSE - close everything first so that Actors sweepers get populated with Clean messages
    sweeper.dbFiles.foreach(_.close())
    sweeper.mapFiles.foreach(_.close())
    sweeper.maps.foreach(_.delete().get)
    sweeper.segments.foreach(_.close)
    sweeper.sweepables.foreach(_.close())
    sweeper.levels.foreach(_.close().await(30.seconds))

    //TERMINATE - terminate all initialised actors
    sweeper.keyValueMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.allMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.blockMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.cacheMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
    sweeper.fileSweepers.foreach(_.get().foreach(sweeper => FileSweeper.closeSync()(sweeper, Bag.less)))
    sweeper.cleaners.foreach(_.get().foreach(cleaner => ByteBufferSweeper.closeSync(10.seconds)(cleaner, Bag.less, TestExecutionContext.executionContext)))
    sweeper.blockCaches.foreach(_.get().foreach(BlockCache.close))

    //DELETE - delete after closing Levels.
    sweeper.levels.foreach(_.delete().await(30.seconds))

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

    sweeper.sweepables.foreach(_.delete())
    sweeper.dbFiles.foreach(_.delete())

    sweeper.paths.foreach {
      path =>
        eventual(10.seconds)(Effect.walkDelete(path))
    }
  }

  private def receiveAll(sweeper: TestCaseSweeper): Unit = {
    sweeper.keyValueMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Bag.Less]()))))
    sweeper.allMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Bag.Less]))))
    sweeper.blockMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Bag.Less]))))
    sweeper.cacheMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Bag.Less]))))
    sweeper.fileSweepers.foreach(_.get().foreach(_.receiveAllForce[Bag.Less]))
    sweeper.cleaners.foreach(_.get().foreach(_.actor.receiveAllForce[Bag.Less]))
    sweeper.blockCaches.foreach(_.get().foreach(_.foreach(_.sweeper.actor.foreach(_.receiveAllForce[Bag.Less]))))
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

  implicit class TestMapFilesSweeperImplicits[M <: swaydb.core.map.Map[_, _, _, _]](map: M) {
    def sweep()(implicit sweeper: TestCaseSweeper): M =
      sweeper sweepMap map
  }

  implicit class TestMapsSweeperImplicits[M <: swaydb.core.map.Maps[_, _, _, _]](map: M) {
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

  implicit class AllMemorySweeperImplicits(keyValue: MemorySweeper.All) {
    def sweep()(implicit sweeper: TestCaseSweeper): MemorySweeper.All =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BufferCleanerSweeperImplicits(keyValue: ByteBufferSweeperActor) {
    def sweep()(implicit sweeper: TestCaseSweeper): ByteBufferSweeperActor =
      sweeper sweepBufferCleaner keyValue
  }

  implicit class FileCleanerSweeperImplicits(keyValue: FileSweeperActor) {
    def sweep()(implicit sweeper: TestCaseSweeper): FileSweeperActor =
      sweeper sweepFileSweeper keyValue
  }

  implicit class FileCleanerCacheSweeperImplicits(cache: CacheNoIO[Unit, FileSweeperActor]) {
    def sweep()(implicit sweeper: TestCaseSweeper): CacheNoIO[Unit, FileSweeperActor] =
      sweeper sweepFileSweeper cache
  }

  implicit class SchedulerSweeperImplicits(scheduler: Scheduler) {
    def sweep()(implicit sweeper: TestCaseSweeper): Scheduler =
      sweeper sweepScheduler scheduler
  }

  implicit class ActorsSweeperImplicits[T, S](actor: ActorRef[T, S]) {
    def sweep()(implicit sweeper: TestCaseSweeper): ActorRef[T, S] =
      sweeper sweepActor actor
  }

  implicit class ActorWiresSweeperImplicits[T, S](actor: ActorWire[T, S]) {
    def sweep()(implicit sweeper: TestCaseSweeper): ActorWire[T, S] =
      sweeper sweepWireActor actor
  }

  implicit class SweepableSweeperImplicits[BAG[_], T](sweepable: T) {
    def sweep()(implicit sweeper: TestCaseSweeper,
                evd: T <:< Sweepable[BAG]): T =
      sweeper sweepSweepable sweepable
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

class TestCaseSweeper(private val fileSweepers: ListBuffer[CacheNoIO[Unit, FileSweeperActor]],
                      private val cleaners: ListBuffer[CacheNoIO[Unit, ByteBufferSweeperActor]],
                      private val blockCaches: ListBuffer[CacheNoIO[Unit, Option[BlockCache.State]]],
                      private val allMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.All]]],
                      private val keyValueMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.KeyValue]]],
                      private val blockMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Block]]],
                      private val cacheMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.Cache]]],
                      private val schedulers: ListBuffer[CacheNoIO[Unit, Scheduler]],
                      private val levels: ListBuffer[LevelRef],
                      private val segments: ListBuffer[Segment],
                      private val mapFiles: ListBuffer[map.Map[_, _, _, _]],
                      private val maps: ListBuffer[Maps[_, _, _, _]],
                      private val dbFiles: ListBuffer[DBFile],
                      private val paths: ListBuffer[Path],
                      private val actors: ListBuffer[ActorRef[_, _]],
                      private val actorWires: ListBuffer[ActorWire[_, _]],
                      private val sweepables: ListBuffer[Sweepable[Any]]) {


  implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.DefaultApplier
  implicit lazy val fileSweeper: FileSweeperActor = fileSweepers.head.value(())
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

  def sweepMap[M <: swaydb.core.map.Map[_, _, _, _]](map: M): M = {
    mapFiles += map
    map
  }

  def sweepMaps[M <: Maps[_, _, _, _]](maps: M): M = {
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

  def sweepBufferCleaner(bufferCleaner: ByteBufferSweeperActor): ByteBufferSweeperActor =
    removeReplaceCache(cleaners, bufferCleaner)

  def sweepFileSweeper(actor: FileSweeperActor): FileSweeperActor =
    removeReplaceCache(fileSweepers, actor)

  def sweepFileSweeper(actor: CacheNoIO[Unit, FileSweeperActor]): CacheNoIO[Unit, FileSweeperActor] = {
    //if previous cache is uninitialised overwrite it with this new one
    if (fileSweepers.lastOption.exists(_.get().isEmpty))
      fileSweepers.remove(0)

    fileSweepers += actor
    actor
  }

  def sweepScheduler(schedule: Scheduler): Scheduler =
    removeReplaceCache(schedulers, schedule)

  def sweepActor[T, S](actor: ActorRef[T, S]): ActorRef[T, S] = {
    actors += actor
    actor
  }

  def sweepWireActor[T, S](actor: ActorWire[T, S]): ActorWire[T, S] = {
    actorWires += actor
    actor
  }

  def sweepSweepable[BAG[_], T](sweepable: T)(implicit ev: T <:< Sweepable[BAG]): T = {
    sweepables += sweepable.asInstanceOf[Sweepable[Any]]
    sweepable
  }

  /**
   * Terminates all sweepers immediately.
   */
  def terminateNow(): Unit =
    TestCaseSweeper.terminate(this)

  def receiveAll(): Unit =
    TestCaseSweeper.receiveAll(this)
}