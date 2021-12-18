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

package swaydb.core.log

import org.scalatest.matchers.should.Matchers._
import org.scalatest.PrivateMethodTester._
import swaydb.{Bag, Glass, TestExecutionContext}
import swaydb.config.{Atomic, MMAP, OptimiseWrites}
import swaydb.config.CoreConfigTestKit._
import swaydb.core.{CoreSpecType, CoreTestSweeper, TestForceSave}
import swaydb.core.CoreTestSweeper._
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.{ByteBufferSweeperActor, State}
import swaydb.core.file.CoreFileTestKit.randomIntDirectory
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.level.NextLevel
import swaydb.core.log.counter.{CounterLog, PersistentCounterLog}
import swaydb.core.log.serialiser.{LogEntryReader, LogEntryWriter}
import swaydb.core.log.timer.Timer
import swaydb.core.log.timer.Timer.PersistentTimer
import swaydb.core.queue.VolatileQueue
import swaydb.core.segment.{CoreFunctionStore, Segment, SegmentOption}
import swaydb.core.segment.data.{KeyValue, Memory}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.effect.Effect
import swaydb.effect.IOValues._
import swaydb.slice.{Slice, SliceOption}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.utils.{Extension, OperatingSystem}
import swaydb.utils.StorageUnits._
import swaydb.Bag.Async
import swaydb.core.file.sweeper.FileSweeper

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

object LogTestKit {

  def getLogs[K, V, C <: LogCache[K, V]](logs: Logs[K, V, C]): VolatileQueue[Log[K, V, C]] =
    logs invokePrivate PrivateMethod[VolatileQueue[Log[K, V, C]]](Symbol("queue"))()

  def getFunctionStore(level: NextLevel): CoreFunctionStore =
    level invokePrivate PrivateMethod[CoreFunctionStore](Symbol("functionStore"))()

  def getTimer[K, V, C <: LogCache[K, V]](logs: Logs[K, V, C]): Timer =
    logs invokePrivate PrivateMethod[Timer](Symbol("timer"))()

  implicit class SliceKeyValueImplicits(actual: Iterable[KeyValue]) {
    def toLogEntry(implicit serialiser: LogEntryWriter[LogEntry.Put[Slice[Byte], Memory]]) =
    //LevelZero does not write Groups therefore this unzip is required.
      actual.foldLeft(Option.empty[LogEntry[Slice[Byte], Memory]]) {
        case (logEntry, keyValue) =>
          val newEntry = LogEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue.toMemory())
          logEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  //windows requires special handling. If a Log was initially opened as a MMAP file
  //we cant reopen without cleaning it first because reopen will re-read the file's content
  //using FileChannel, and flushes it's content to a new file deleting the old File. But if it's
  //was previously opened as MMAP file (in a test) then it cannot be deleted without clean.
  def ensureCleanedForWindows(mmap: MMAP.Log)(implicit bufferCleaner: ByteBufferSweeperActor): Unit =
    if (OperatingSystem.isWindows() && mmap.isMMAP) {
      val cleaner = bufferCleaner.actor()
      val state = cleaner.receiveAllForce[Glass, State](state => state)
      eventual(30.seconds)(state.pendingClean.isEmpty shouldBe true)
    }

  //cannot be added to TestBase because PersistentMap cannot leave the log package.
  implicit class ReopenLog[K, V, C <: LogCache[K, V]](log: PersistentLog[K, V, C]) {
    def reopen(implicit keyOrder: KeyOrder[K],
               reader: LogEntryReader[LogEntry[K, V]],
               testCaseSweeper: CoreTestSweeper,
               logCacheBuilder: LogCacheBuilder[C]) = {
      log.close()

      implicit val writer: LogEntryWriter[LogEntry.Put[K, V]] = log.writer
      implicit val forceSaveApplied: ForceSaveApplier = log.forceSaveApplier
      implicit val cleaner: ByteBufferSweeperActor = log.bufferCleaner
      implicit val sweeper: FileSweeper = log.fileSweeper

      ensureCleanedForWindows(log.mmap)

      Log.persistent[K, V, C](
        folder = log.path,
        mmap = MMAP.randomForLog(),
        flushOnOverflow = log.flushOnOverflow,
        fileSize = log.fileSize,
        dropCorruptedTailEntries = false
      ).runRandomIO.get.item.sweep()
    }
  }

  implicit class PersistentLogImplicit[K, V](log: PersistentLog[K, V, _]) {
    /**
     * Manages closing of Log accounting for Windows where
     * Memory-mapped files require in-memory ByteBuffer be cleared.
     */
    def ensureClose(): Unit = {
      log.close()
      ensureCleanedForWindows(log.mmap)(log.bufferCleaner)

      implicit val ec: ExecutionContext = TestExecutionContext.executionContext
      implicit val bag: Async.Retryable[Future] = Bag.future
      val isShut = (log.bufferCleaner.actor() ask ByteBufferCommand.IsTerminated[Unit]).await(10.seconds)
      assert(isShut, "Is not shut")
    }
  }

  //cannot be added to TestBase because PersistentMap cannot leave the log package.
  implicit class ReopenCounter(counter: PersistentCounterLog) {
    def reopen(implicit bufferCleaner: ByteBufferSweeperActor,
               forceSaveApplier: ForceSaveApplier,
               writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
               reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]],
               testCaseSweeper: CoreTestSweeper): PersistentCounterLog = {
      counter.close()
      ensureCleanedForWindows(counter.mmap)

      CounterLog.persistent(
        dir = counter.path,
        fileSize = counter.fileSize,
        mmap = MMAP.randomForLog(),
        mod = counter.mod
      ).get.sweep()
    }
  }

  implicit class ReopenTimer(timer: PersistentTimer) {
    def reopen(implicit bufferCleaner: ByteBufferSweeperActor,
               forceSaveApplier: ForceSaveApplier,
               writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
               reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]],
               testCaseSweeper: CoreTestSweeper): PersistentTimer = {
      timer.close()
      ensureCleanedForWindows(timer.counter.mmap)

      val newTimer =
        Timer.persistent(
          path = timer.counter.path.getParent,
          mmap = MMAP.randomForLog(),
          mod = timer.counter.mod,
          fileSize = timer.counter.fileSize
        ).get

      newTimer.counter.sweep()

      newTimer
    }
  }


  implicit class LogsImplicit[OK, OV, K <: OK, V <: OV](logs: Logs[K, V, _]) {

    /**
     * Manages closing of Map accounting for Windows where
     * Memory-mapped files require in-memory ByteBuffer be cleared.
     */
    def ensureClose(): Unit = {
      implicit val ec = TestExecutionContext.executionContext
      implicit val bag = Bag.future
      logs.close().get
      logs.bufferCleaner.actor().receiveAllForce[Glass, Unit](_ => ())
      (logs.bufferCleaner.actor() ask ByteBufferCommand.IsTerminated[Unit]).await(10.seconds)
    }
  }

  implicit class LogEntryImplicits(actual: LogEntry[Slice[Byte], Memory]) {

    def shouldBe(expected: LogEntry[Slice[Byte], Memory]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual.totalByteSize shouldBe expected.totalByteSize
      actual match {
        case LogEntry.Put(key, value) =>
          val exp = expected.asInstanceOf[LogEntry.Put[Slice[Byte], Memory]]
          key shouldBe exp.key
          value shouldBe exp.value

        case LogEntry.Remove(key) =>
          val exp = expected.asInstanceOf[LogEntry.Remove[Slice[Byte]]]
          key shouldBe exp.key

        case batch: LogEntry.Batch[Slice[Byte], Memory] => //LogEntry is a batch of other MapEntries, iterate and assert.
          expected.entries.size shouldBe batch.entries.size
          expected.entries.zip(batch.entries) foreach {
            case (expected, actual) =>
              actual shouldBe expected
          }
      }
    }
  }

  implicit class SegmentsPersistentMapImplicits(actual: LogEntry[Slice[Byte], Segment]) {

    def shouldBe(expected: LogEntry[Slice[Byte], Segment]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize

      val actualMap = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
      actual.applyBatch(actualMap)

      val expectedMap = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
      expected.applyBatch(expectedMap)

      actualMap.size shouldBe expectedMap.size

      val actualArray = actualMap.toIterable.toArray
      val expectedArray = expectedMap.toIterable.toArray

      actualArray.indices.foreach {
        i =>
          val actual = actualArray(i)
          val expected = expectedArray(i)
          actual._1 shouldBe expected._1
          actual._2 shouldBe expected._2
      }
    }
  }

  def testLogFilePath()(implicit sweeper: CoreTestSweeper,
                        coreSpecType: CoreSpecType): Path =
    if (coreSpecType.isMemory)
      randomIntDirectory()
        .resolve(sweeper.idGenerator.nextId().toString + Extension.Log.dotToString)
        .sweep()
    else
      Effect
        .createDirectoriesIfAbsent(randomIntDirectory())
        .resolve(sweeper.idGenerator.nextId().toString + Extension.Log.dotToString)
        .sweep()

  object TestLog {
    def apply(keyValues: Slice[Memory],
              fileSize: Int = 4.mb,
              flushOnOverflow: Boolean = false,
              mmap: MMAP.Log = MMAP.On(OperatingSystem.isWindows(), TestForceSave.mmap()))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                           timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                           sweeper: CoreTestSweeper,
                                                                                           coreSpecType: CoreSpecType): LevelZeroLog = {
      import swaydb.core.log.serialiser.LevelZeroLogEntryWriter._
      import sweeper._

      implicit val optimiseWrites: OptimiseWrites = OptimiseWrites.random
      implicit val atomic: Atomic = Atomic.random

      val testLog =
        if (coreSpecType.isMemory)
          Log.memory[Slice[Byte], Memory, LevelZeroLogCache](
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow
          )
        else
          Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
            folder = testLogFilePath(),
            mmap = mmap,
            flushOnOverflow = flushOnOverflow,
            fileSize = fileSize
          ).runRandomIO.get

      keyValues foreach {
        keyValue =>
          testLog.writeSync(LogEntry.Put(keyValue.key, keyValue))
      }

      testLog.sweep()
    }
  }

}
