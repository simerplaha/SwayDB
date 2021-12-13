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
import swaydb.{ActorRef, Bag, DefActor, Glass, Scheduler}
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.CoreTestSweepers._
import swaydb.core.cache.{Cache, CacheUnsafe}
import swaydb.core.file.{CoreFile, ForceSaveApplier}
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.level.LevelRef
import swaydb.core.log.{Log, Logs}
import swaydb.core.log.counter.CounterLog
import swaydb.core.segment.io.SegmentCompactionIO
import swaydb.core.segment.Segment
import swaydb.core.segment.block.{BlockCache, BlockCacheState}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.effect.Effect
import swaydb.testkit.RunThis._
import swaydb.utils.{IDGenerator, OperatingSystem}

import java.nio.file.{Path, Paths}
import java.util.concurrent.atomic.AtomicInteger
import scala.beans.BeanProperty
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

object TestSweeper extends LazyLogging {

  private val testNumber = new AtomicInteger(0)

  private val projectTargetFolder: String =
    getClass.getClassLoader.getResource("").getPath

  val projectFolder: Path =
    if (OperatingSystem.isWindows)
      Paths.get(projectTargetFolder.drop(1)).getParent.getParent
    else
      Paths.get(projectTargetFolder).getParent.getParent

  val testFolder: Path =
    projectFolder.resolve("TEST_FILES")

  val testMemoryFolder: Path =
    projectFolder.resolve("TEST_MEMORY_FILES")

  def deleteParentPath(path: Path) = {
    val parentPath = path.getParent
    //also delete parent folder of Segment. TestSegments are created with a parent folder.
    if (Effect.exists(parentPath) && Try(parentPath.getFileName.toString.toInt).isSuccess)
      Effect.walkDelete(parentPath)
  }

  private def terminate(sweeper: TestSweeper): Unit = {
    logger.info(s"Terminating ${classOf[TestSweeper].getSimpleName}")

    sweeper.actors.foreach(_.terminateAndClear[Glass]())
    sweeper.defActors.foreach(_.terminateAndClear[Glass]())

    sweeper.schedulers.foreach(_.get().foreach(_.terminate()))

    //CLOSE - close everything first so that Actors sweepers get populated with Clean messages
    sweeper.coreFiles.foreach(_.close())
    sweeper.mapFiles.foreach(_.close())
    sweeper.logs.foreach(_.delete().get)
    sweeper.segments.foreach(_.close())
    sweeper.levels.foreach(_.close[Glass]())
    sweeper.counters.foreach(_.close())

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
    if (sweeper.deleteFiles) sweeper.levels.foreach(_.delete[Glass]())

    sweeper.segments.foreach {
      segment =>
        if (sweeper.deleteFiles && segment.existsOnDisk())
          segment.delete()
        else
          segment.close() //the test itself might've delete this file so submit close just in-case.

        //eventual because segment.delete() goes to an actor which might eventually get resolved.
        if (sweeper.deleteFiles)
          eventual(20.seconds)(deleteParentPath(segment.path))
    }

    if (sweeper.deleteFiles) {
      sweeper.mapFiles.foreach {
        map =>
          if (map.pathOption.exists(Effect.exists))
            map.pathOption.foreach(Effect.walkDelete)
          map.pathOption.foreach(deleteParentPath)
      }

      sweeper.coreFiles.foreach(_.delete())
      sweeper.functions.foreach(_ ())

      sweeper.paths.foreach {
        path =>
          eventual(10.seconds)(Effect.walkDelete(path))
      }

      eventual(10.seconds)(sweeper.testDirPath)
    }
  }

  private def receiveAll(sweeper: TestSweeper): Unit = {
    sweeper.keyValueMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.allMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.blockMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.cacheMemorySweepers.foreach(_.get().foreach(_.foreach(_.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
    sweeper.fileSweepers.foreach(_.get().foreach {
      actor =>
        actor.closer.receiveAllForce[Glass, Unit](_ => ())
        actor.deleter.receiveAllForce[Glass, Unit](_ => ())
    })
    sweeper.cleaners.foreach(_.get().foreach(_.actor().receiveAllForce[Glass, Unit](_ => ())))
    sweeper.blockCaches.foreach(_.get().foreach(_.foreach(_.sweeper.actor.foreach(_.receiveAllForce[Glass, Unit](_ => ())))))
  }

  def apply(times: Int, log: Boolean = true)(code: TestSweeper => Unit): Unit = {
    import swaydb.testkit.RunThis._
    runThis(times, log)(TestSweeper[Unit](s"TEST${testNumber.incrementAndGet()}")(code))
  }

  def apply[T](code: TestSweeper => T): T =
    TestSweeper[T](s"TEST${testNumber.incrementAndGet()}")(code)

  def apply[T](testName: String)(code: TestSweeper => T): T = {
    val sweeper = new TestSweeper(testName)
    val result = code(sweeper)
    terminate(sweeper)
    result
  }

  implicit class TestLevelLevelSweeperImplicits[L <: LevelRef](level: L) {
    def sweep()(implicit sweeper: TestSweeper): L =
      sweeper sweepLevel level
  }

  implicit class TestLevelSegmentSweeperImplicits[L <: Segment](segment: L) {
    def sweep()(implicit sweeper: TestSweeper): L =
      sweeper sweepSegment segment
  }

  implicit class TestMapFilesSweeperImplicits[M <: Log[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestSweeper): M =
      sweeper sweepLog map
  }

  implicit class TestMapsSweeperImplicits[M <: Logs[_, _, _]](map: M) {
    def sweep()(implicit sweeper: TestSweeper): M =
      sweeper sweepLogs map
  }

  implicit class TestLevelPathSweeperImplicits(path: Path) {
    def sweep()(implicit sweeper: TestSweeper): Path =
      sweeper sweepPath path
  }

  implicit class CoreFileSweeperImplicits(coreFile: CoreFile) {
    def sweep()(implicit sweeper: TestSweeper): CoreFile =
      sweeper sweepCoreFiles coreFile
  }

  implicit class KeyValueMemorySweeperImplicits(keyValue: MemorySweeper.KeyValue) {
    def sweep()(implicit sweeper: TestSweeper): MemorySweeper.KeyValue =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockSweeperImplicits(keyValue: MemorySweeper.Block) {
    def sweep()(implicit sweeper: TestSweeper): MemorySweeper.Block =
      sweeper sweepMemorySweeper keyValue
  }

  implicit class BlockCacheStateSweeperImplicits(state: BlockCacheState) {
    def sweep()(implicit sweeper: TestSweeper): BlockCacheState =
      sweeper sweepBlockCacheState state
  }

  implicit class AllMemorySweeperImplicits(actor: MemorySweeper.All) {
    def sweep()(implicit sweeper: TestSweeper): MemorySweeper.All =
      sweeper sweepMemorySweeper actor
  }

  implicit class BufferCleanerSweeperImplicits(actor: ByteBufferSweeperActor) {
    def sweep()(implicit sweeper: TestSweeper): ByteBufferSweeperActor =
      sweeper sweepBufferCleaner actor
  }

  implicit class CompactionIOActorImplicits(actor: SegmentCompactionIO.Actor) {
    def sweep()(implicit sweeper: TestSweeper): SegmentCompactionIO.Actor =
      sweeper sweepCompactionIOActors actor
  }

  implicit class FileCleanerSweeperImplicits(actor: FileSweeper.On) {
    def sweep()(implicit sweeper: TestSweeper): FileSweeper.On =
      sweeper sweepFileSweeper actor
  }

  implicit class SchedulerSweeperImplicits(scheduler: Scheduler) {
    def sweep()(implicit sweeper: TestSweeper): Scheduler =
      sweeper sweepScheduler scheduler
  }

  implicit class ActorsSweeperImplicits[T, S](actor: ActorRef[T, S]) {
    def sweep()(implicit sweeper: TestSweeper): ActorRef[T, S] =
      sweeper sweepActor actor
  }

  implicit class CountersSweeperImplicits[C <: CounterLog](counter: C) {
    def sweep()(implicit sweeper: TestSweeper): C =
      sweeper sweepCounter counter
  }

  implicit class ActorWiresSweeperImplicits[T](actor: DefActor[T]) {
    def sweep()(implicit sweeper: TestSweeper): DefActor[T] =
      sweeper sweepActor actor
  }

  implicit class FunctionSweeperImplicits[BAG[_], T](sweepable: T) {
    def sweep(f: T => Unit)(implicit sweeper: TestSweeper): T =
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

class TestSweeper(val testName: String = s"TEST${TestSweeper.testNumber.incrementAndGet()}",
                  @BeanProperty protected var deleteFiles: Boolean = true,
                  private val fileSweepers: ListBuffer[CacheUnsafe[Unit, FileSweeper.On]] = ListBuffer(Cache.unsafe[Unit, FileSweeper.On](true, true, None)((_, _) => createFileSweeper())),
                  private val cleaners: ListBuffer[CacheUnsafe[Unit, ByteBufferSweeperActor]] = ListBuffer(Cache.unsafe[Unit, ByteBufferSweeperActor](true, true, None)((_, _) => createBufferCleaner())),
                  private val compactionIOActors: ListBuffer[CacheUnsafe[Unit, SegmentCompactionIO.Actor]] = ListBuffer(Cache.unsafe[Unit, SegmentCompactionIO.Actor](true, true, None)((_, _) => SegmentCompactionIO.create()(TestExecutionContext.executionContext))),
                  private val blockCaches: ListBuffer[CacheUnsafe[Unit, Option[BlockCacheState]]] = ListBuffer(Cache.unsafe[Unit, Option[BlockCacheState]](true, true, None)((_, _) => createBlockCacheRandom())),
                  private val allMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.All]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.All]](true, true, None)((_, _) => createMemorySweeperMax())),
                  private val keyValueMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.KeyValue]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom())),
                  private val blockMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.Block]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.Block]](true, true, None)((_, _) => createMemoryBlockSweeper())),
                  private val cacheMemorySweepers: ListBuffer[CacheUnsafe[Unit, Option[MemorySweeper.Cache]]] = ListBuffer(Cache.unsafe[Unit, Option[MemorySweeper.Cache]](true, true, None)((_, _) => createRandomCacheSweeper())),
                  private val schedulers: ListBuffer[CacheUnsafe[Unit, Scheduler]] = ListBuffer(Cache.unsafe[Unit, Scheduler](true, true, None)((_, _) => Scheduler()(DefaultExecutionContext.sweeperEC))),
                  private val levels: ListBuffer[LevelRef] = ListBuffer.empty,
                  private val segments: ListBuffer[Segment] = ListBuffer.empty,
                  private val mapFiles: ListBuffer[Log[_, _, _]] = ListBuffer.empty,
                  private val logs: ListBuffer[Logs[_, _, _]] = ListBuffer.empty,
                  private val coreFiles: ListBuffer[CoreFile] = ListBuffer.empty,
                  private val paths: ListBuffer[Path] = ListBuffer.empty,
                  private val actors: ListBuffer[ActorRef[_, _]] = ListBuffer.empty,
                  private val defActors: ListBuffer[DefActor[_]] = ListBuffer.empty,
                  private val counters: ListBuffer[CounterLog] = ListBuffer.empty,
                  private val functions: ListBuffer[() => Unit] = ListBuffer.empty) {

  val idGenerator = IDGenerator()

  val testDirPath: Path =
    TestSweeper.testFolder.resolve(testName)

  def testDir(): Path =
    Effect.createDirectoriesIfAbsent(testDirPath)

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

  implicit lazy val scheduler = schedulers.head.getOrFetch(())

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

  private def removeReplaceCache[I, O](sweepers: ListBuffer[CacheUnsafe[I, O]], replace: O): O = {
    if (sweepers.lastOption.exists(_.get().isEmpty))
      sweepers.remove(0)

    val cache = Cache.unsafe[I, O](true, true, Some(replace))((_, _) => replace)
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

  def sweepScheduler(schedule: Scheduler): Scheduler =
    removeReplaceCache(schedulers, schedule)

  def sweepCompactionIOActors(actor: SegmentCompactionIO.Actor): SegmentCompactionIO.Actor =
    removeReplaceCache(compactionIOActors, actor)

  def sweepActor[T, S](actor: ActorRef[T, S]): ActorRef[T, S] = {
    actors += actor
    actor
  }

  def sweepActor[T](actor: DefActor[T]): DefActor[T] = {
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
    TestSweeper.terminate(this)

  def receiveAll(): Unit =
    TestSweeper.receiveAll(this)
}
