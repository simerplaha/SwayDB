///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.log
//
//import org.scalatest.matchers.should.Matchers._
//import swaydb.IOValues._
//import swaydb.config.MMAP
//import swaydb.core.TestSweeper._
//import swaydb.core.CoreTestData._
//import swaydb.core.file.ForceSaveApplier
//import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
//import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.{ByteBufferSweeperActor, State}
//import swaydb.core.log.counter.{CounterLog, PersistentCounterLog}
//import swaydb.core.log.serialiser.{LogEntryReader, LogEntryWriter}
//import swaydb.core.log.timer.Timer
//import swaydb.core.log.timer.Timer.PersistentTimer
//import swaydb.core.{TestSweeper, TestExecutionContext}
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//import swaydb.{Bag, Glass}
//
//import scala.concurrent.duration.DurationInt
//
//object LogTestUtil {
//
//  //windows requires special handling. If a Log was initially opened as a MMAP file
//  //we cant reopen without cleaning it first because reopen will re-read the file's content
//  //using FileChannel, and flushes it's content to a new file deleting the old File. But if it's
//  //was previously opened as MMAP file (in a test) then it cannot be deleted without clean.
//  def ensureCleanedForWindows(mmap: MMAP.Log)(implicit bufferCleaner: ByteBufferSweeperActor): Unit =
//    if (OperatingSystem.isWindows() && mmap.isMMAP) {
//      val cleaner = bufferCleaner.actor()
//      val state = cleaner.receiveAllForce[Glass, State](state => state)
//      eventual(30.seconds)(state.pendingClean.isEmpty shouldBe true)
//    }
//
//  //cannot be added to TestBase because PersistentMap cannot leave the log package.
//  implicit class ReopenLog[K, V, C <: LogCache[K, V]](log: PersistentLog[K, V, C]) {
//    def reopen(implicit keyOrder: KeyOrder[K],
//               reader: LogEntryReader[LogEntry[K, V]],
//               testCaseSweeper: TestSweeper,
//               logCacheBuilder: LogCacheBuilder[C]) = {
//      log.close()
//
//      implicit val writer = log.writer
//      implicit val forceSaveApplied = log.forceSaveApplier
//      implicit val cleaner = log.bufferCleaner
//      implicit val sweeper = log.fileSweeper
//
//      ensureCleanedForWindows(log.mmap)
//
//      Log.persistent[K, V, C](
//        folder = log.path,
//        mmap = MMAP.randomForLog(),
//        flushOnOverflow = log.flushOnOverflow,
//        fileSize = log.fileSize,
//        dropCorruptedTailEntries = false
//      ).runRandomIO.right.value.item.sweep()
//    }
//  }
//
//  implicit class PersistentLogImplicit[K, V](log: PersistentLog[K, V, _]) {
//    /**
//     * Manages closing of Log accounting for Windows where
//     * Memory-mapped files require in-memory ByteBuffer be cleared.
//     */
//    def ensureClose(): Unit = {
//      log.close()
//      ensureCleanedForWindows(log.mmap)(log.bufferCleaner)
//
//      implicit val ec = TestExecutionContext.executionContext
//      implicit val bag = Bag.future
//      val isShut = (log.bufferCleaner.actor() ask ByteBufferCommand.IsTerminated[Unit]).await(10.seconds)
//      assert(isShut, "Is not shut")
//    }
//  }
//
//  //cannot be added to TestBase because PersistentMap cannot leave the log package.
//  implicit class ReopenCounter(counter: PersistentCounterLog) {
//    def reopen(implicit bufferCleaner: ByteBufferSweeperActor,
//               forceSaveApplier: ForceSaveApplier,
//               writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
//               reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]],
//               testCaseSweeper: TestSweeper): PersistentCounterLog = {
//      counter.close()
//      ensureCleanedForWindows(counter.mmap)
//
//      CounterLog.persistent(
//        dir = counter.path,
//        fileSize = counter.fileSize,
//        mmap = MMAP.randomForLog(),
//        mod = counter.mod
//      ).value.sweep()
//    }
//  }
//
//  implicit class ReopenTimer(timer: PersistentTimer) {
//    def reopen(implicit bufferCleaner: ByteBufferSweeperActor,
//               forceSaveApplier: ForceSaveApplier,
//               writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
//               reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]],
//               testCaseSweeper: TestSweeper): PersistentTimer = {
//      timer.close()
//      ensureCleanedForWindows(timer.counter.mmap)
//
//      val newTimer =
//        Timer.persistent(
//          path = timer.counter.path.getParent,
//          mmap = MMAP.randomForLog(),
//          mod = timer.counter.mod,
//          fileSize = timer.counter.fileSize
//        ).value
//
//      newTimer.counter.sweep()
//
//      newTimer
//    }
//  }
//}
