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

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Bag, IO, OK}
import swaydb.core.RunThis._
import swaydb.core.TestSweeper._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.core.cache.{Cache, CacheNoIO}
import swaydb.core.io.file.{BlockCache, Effect}
import swaydb.core.level.LevelRef
import swaydb.core.segment.Segment

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
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

  def apply(): TestCaseSweeper = {
    new TestCaseSweeper(
      keyValueMemorySweepers = ListBuffer(Cache.noIO[Unit, Option[MemorySweeper.KeyValue]](true, true, None)((_, _) => createMemorySweeperRandom())),
      fileSweepers = ListBuffer(Cache.noIO[Unit, FileSweeperActor](true, true, None)((_, _) => createFileSweeper())),
      cleaners = ListBuffer(Cache.noIO[Unit, ByteBufferSweeperActor](true, true, None)((_, _) => createBufferCleaner())),
      blockCaches = ListBuffer(Cache.noIO[Unit, Option[BlockCache.State]](true, true, None)((_, _) => createBlockCacheRandom())),
      levels = ListBuffer.empty,
      segments = ListBuffer.empty,
      maps = ListBuffer.empty,
      paths = ListBuffer.empty
    )
  }

  def deleteParentPath(path: Path) = {
    val parentPath = path.getParent
    //also delete parent folder of Segment. TestSegments are created with a parent folder.
    if (Effect.exists(parentPath) && Try(parentPath.getFileName.toString.toInt).isSuccess)
      Effect.walkDelete(parentPath)
  }

  def apply[T](code: TestCaseSweeper => T): T = {
    val sweeper = TestCaseSweeper()
    try
      code(sweeper)
    finally {
      implicit val bag = Bag.future(TestExecutionContext.executionContext)

      def future = Future.sequence(sweeper.levels.map(_.delete(5.second)))

      IO.fromFuture(future).run(0).await(10.seconds)

      sweeper.segments.foreach {
        segment =>
          if (segment.existsOnDisk)
            segment.delete
          deleteParentPath(segment.path)
      }

      sweeper.maps.foreach {
        map =>
          if (map.pathOption.exists(Effect.exists))
            map.delete
          map.pathOption.foreach(deleteParentPath)
      }

      //calling this after since delete would've already invoked these.
      sweeper.keyValueMemorySweepers.foreach(_.get().foreach(MemorySweeper.close))
      sweeper.fileSweepers.foreach(_.get().foreach(sweeper => FileSweeper.closeSync(1.second)(sweeper, Bag.less)))
      sweeper.cleaners.foreach(_.get().foreach(cleaner => ByteBufferSweeper.closeSync(1.second, 10.seconds)(cleaner, Bag.less, TestExecutionContext.executionContext)))
      sweeper.blockCaches.foreach(_.get().foreach(BlockCache.close))

      sweeper.paths.foreach(Effect.deleteIfExists)
    }
  }


  implicit class TestLevelLevelSweeperImplicits[L <: LevelRef](level: L) {
    def clean()(implicit sweeper: TestCaseSweeper): L =
      sweeper cleanLevel level
  }

  implicit class TestLevelSegmentSweeperImplicits[L <: Segment](segment: L) {
    def clean()(implicit sweeper: TestCaseSweeper): L =
      sweeper cleanSegment segment
  }

  implicit class TestMapsSweeperImplicits[OK, OV, K <: OK, V <: OV](map: swaydb.core.map.Map[OK, OV, K, V]) {
    def clean()(implicit sweeper: TestCaseSweeper): swaydb.core.map.Map[OK, OV, K, V] =
      sweeper cleanMap map
  }

  implicit class TestLevelPathSweeperImplicits(path: Path) {
    def clean()(implicit sweeper: TestCaseSweeper): Path =
      sweeper cleanPath path
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

class TestCaseSweeper private(private val keyValueMemorySweepers: ListBuffer[CacheNoIO[Unit, Option[MemorySweeper.KeyValue]]],
                              private val fileSweepers: ListBuffer[CacheNoIO[Unit, FileSweeperActor]],
                              private val cleaners: ListBuffer[CacheNoIO[Unit, ByteBufferSweeperActor]],
                              private val blockCaches: ListBuffer[CacheNoIO[Unit, Option[BlockCache.State]]],
                              private val levels: ListBuffer[LevelRef],
                              private val segments: ListBuffer[Segment],
                              private val maps: ListBuffer[map.Map[_, _, _, _]],
                              private val paths: ListBuffer[Path]) {


  lazy val keyValueMemorySweeper = keyValueMemorySweepers.head.value(())
  lazy val fileSweeper = fileSweepers.head.value(())
  lazy val cleaner = cleaners.head.value(())
  lazy val blockCache = blockCaches.head.value(())

  def cleanLevel[T <: LevelRef](levelRef: T): T = {
    levels += levelRef
    levelRef
  }

  def cleanSegment[S <: Segment](segment: S): S = {
    segments += segment
    segment
  }

  def cleanMap[OK, OV, K <: OK, V <: OV](map: swaydb.core.map.Map[OK, OV, K, V]): swaydb.core.map.Map[OK, OV, K, V] = {
    maps += map
    map
  }

  def cleanPath(path: Path): Path = {
    paths += path
    path
  }
}