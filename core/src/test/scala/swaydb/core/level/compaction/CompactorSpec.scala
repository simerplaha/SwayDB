///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.level.compaction
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.Error.Segment.ErrorHandler
//import swaydb.IO
//import swaydb.core.RunThis._
//import swaydb.core.actor.WiredActor
//import swaydb.core.queue.{FileLimiter, MemorySweeper}
//import swaydb.core.util.Delay
//import swaydb.core.{TestBase, TestExecutionContext, TestLimitQueues, TestTimer}
//import swaydb.data.Reserve
//import swaydb.data.compaction.CompactionExecutionContext
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//
//import scala.collection.mutable
//import scala.concurrent.duration._
//
//class CompactorSpec0 extends CompactorSpec
//
//class CompactorSpec1 extends CompactorSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = true
//  override def mmapSegmentsOnRead = true
//  override def level0MMAP = true
//  override def appendixStorageMMAP = true
//}
//
//class CompactorSpec2 extends CompactorSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = false
//  override def mmapSegmentsOnRead = false
//  override def level0MMAP = false
//  override def appendixStorageMMAP = false
//}
//
//class CompactorSpec3 extends CompactorSpec {
//  override def inMemoryStorage = true
//}
//
//sealed trait CompactorSpec extends TestBase with MockFactory {
//
//  val keyValueCount = 1000
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val timer = TestTimer.Empty
//
//  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
//  implicit val keyValuesLimitImplicitLimiter: Option[MemorySweeper] = TestLimitQueues.memorySweeper
//
//  implicit val compactionOrdering = DefaultCompactionOrdering
//
//  override def deleteFiles = false
//
//  "createActor" should {
//    "build compaction hierarchy" when {
//      "there are two Levels and one new ExecutionContext" in {
//        val nextLevel = TestLevel()
//        val zero = TestLevelZero(nextLevel = Some(nextLevel))
//
//        val actor =
//          Compactor.createActor(
//            List(zero, nextLevel),
//            List(
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
//              CompactionExecutionContext.Shared
//            )
//          ).get
//
//        actor.unsafeGetState.compactionStates shouldBe empty
//        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
//        actor.unsafeGetState.child shouldBe empty
//
//        zero.delete.get
//      }
//
//      "there are two Levels and two new ExecutionContext" in {
//        val nextLevel = TestLevel()
//        val zero = TestLevelZero(nextLevel = Some(nextLevel))
//
//        val actor =
//          Compactor.createActor(
//            List(zero, nextLevel),
//            List(
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext)
//            )
//          ).get
//
//        actor.unsafeGetState.compactionStates shouldBe empty
//        actor.unsafeGetState.levels.map(_.rootPath) should contain only zero.rootPath
//        actor.unsafeGetState.child shouldBe defined
//
//        val childActor = actor.unsafeGetState.child.get.unsafeGetState
//        childActor.child shouldBe empty
//        childActor.levels.map(_.rootPath) should contain only nextLevel.rootPath
//
//        zero.delete.get
//      }
//
//      "there are three Levels and one new ExecutionContext" in {
//        val nextLevel2 = TestLevel()
//        val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
//        val zero = TestLevelZero(nextLevel = Some(nextLevel))
//
//        val actor =
//          Compactor.createActor(
//            List(zero, nextLevel, nextLevel2),
//            List(
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
//              CompactionExecutionContext.Shared,
//              CompactionExecutionContext.Shared
//            )
//          ).get
//
//        actor.unsafeGetState.compactionStates shouldBe empty
//        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath, nextLevel2.rootPath)
//        actor.unsafeGetState.child shouldBe empty
//
//        zero.delete.get
//      }
//
//      "there are three Levels and two new ExecutionContext" in {
//        val nextLevel2 = TestLevel()
//        val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
//        val zero = TestLevelZero(nextLevel = Some(nextLevel))
//
//        val actor =
//          Compactor.createActor(
//            List(zero, nextLevel, nextLevel2),
//            List(
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
//              CompactionExecutionContext.Shared,
//              CompactionExecutionContext.Create(TestExecutionContext.executionContext)
//            )
//          ).get
//
//        actor.unsafeGetState.compactionStates shouldBe empty
//        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
//        actor.unsafeGetState.child shouldBe defined
//
//        val childActor = actor.unsafeGetState.child.get.unsafeGetState
//        childActor.child shouldBe empty
//        childActor.levels.map(_.rootPath) should contain only nextLevel2.rootPath
//
//        zero.delete.get
//      }
//    }
//  }
//
//  "scheduleNextWakeUp" should {
//    val nextLevel = TestLevel()
//    val level = TestLevel(nextLevel = Some(nextLevel))
//
//    val testState =
//      CompactorState(
//        levels = Slice(level, nextLevel),
//        child = None,
//        ordering = DefaultCompactionOrdering.ordering(_ => LevelCompactionState.Sleep(1.day.fromNow, 0, 0)),
//        executionContext = TestExecutionContext.executionContext,
//        compactionStates = mutable.Map.empty
//      )
//
//    "not trigger wakeUp" when {
//      "level states were empty" in {
//        val compactor = mock[CompactionStrategy[CompactorState]]
//
//        val actor =
//          WiredActor[CompactionStrategy[CompactorState], CompactorState](
//            impl = compactor,
//            state = testState
//          )
//
//        Compactor.scheduleNextWakeUp(
//          state = testState,
//          self = actor
//        )
//
//        sleep(5.seconds)
//      }
//    }
//
//    "trigger wakeUp" when {
//      "one of level states is awaiting pull and successfully received read" in {
//        //create IO.Later that is busy
//        val reserve = Reserve[Unit](())
//        reserve.isBusy shouldBe true
//        val busy = swaydb.Error.ReservedResource(reserve)
//        val later = IO.Deferred((), busy)
//        later.isBusy shouldBe true
//
//        val awaitingPull = LevelCompactionState.AwaitingPull(later, 1.minute.fromNow, 0, 0)
//        awaitingPull.isReady shouldBe false
//        //set the state to be awaiting pull
//        val state =
//          testState.copy(
//            compactionStates =
//              mutable.Map(
//                level -> awaitingPull
//              )
//          )
//        //mock the compaction that should expect a wakeUp call
//        val compactor = mock[CompactionStrategy[CompactorState]]
//        compactor.wakeUp _ expects(*, *, *) onCall {
//          (callState, doCopy, _) =>
//            callState shouldBe state
//            doCopy shouldBe false
//        }
//
//        //initialise Compactor with the mocked class
//        val actor =
//          WiredActor[CompactionStrategy[CompactorState], CompactorState](
//            impl = compactor,
//            state = state
//          )
//
//        Compactor.scheduleNextWakeUp(
//          state = state,
//          self = actor
//        )
//        //after calling scheduleNextWakeUp listener should be initialised.
//        //this ensures that multiple wakeUp callbacks do not value registered for the same pull.
//        awaitingPull.listenerInitialised shouldBe true
//
//        //free the reserve and compaction should expect a message.
//        Delay.future(1.second)(Reserve.setFree(reserve))
//
//        eventual(3.seconds) {
//          //eventually is set to be ready.
//          awaitingPull.isReady shouldBe true
//          //next sleep task is initialised
//          state.sleepTask shouldBe defined
//          //it's the await's timeout.
//          state.sleepTask.get._2 shouldBe awaitingPull.timeout
//        }
//      }
//
//      "one of level states is awaiting pull and other Level's sleep is shorter" in {
//        //create IO.Later that is busy
//        val reserve = Reserve[Unit](())
//        reserve.isBusy shouldBe true
//        val busy = swaydb.Error.ReservedResource(reserve)
//        val later = IO.Deferred((), busy)
//        later.isBusy shouldBe true
//
//        val level1AwaitingPull = LevelCompactionState.AwaitingPull(later, 1.minute.fromNow, 0, 0)
//        level1AwaitingPull.isReady shouldBe false
//
//        //level 2's sleep is shorter than level1's awaitPull timeout sleep.
//        val level2Sleep = LevelCompactionState.Sleep(5.seconds.fromNow, 0, 0)
//        //set the state to be awaiting pull
//        val state =
//          testState.copy(
//            compactionStates =
//              mutable.Map(
//                level -> level1AwaitingPull,
//                nextLevel -> level2Sleep
//              )
//          )
//        //mock the compaction that should expect a wakeUp call
//        val compactor = mock[CompactionStrategy[CompactorState]]
//        compactor.wakeUp _ expects(*, *, *) onCall {
//          (callState, doCopy, _) =>
//            callState shouldBe state
//            doCopy shouldBe false
//        }
//
//        //initialise Compactor with the mocked class
//        val actor =
//          WiredActor[CompactionStrategy[CompactorState], CompactorState](
//            impl = compactor,
//            state = state
//          )
//
//        Compactor.scheduleNextWakeUp(
//          state = state,
//          self = actor
//        )
//        //a callback for awaiting pull should always be initialised.
//        level1AwaitingPull.listenerInitialised shouldBe true
//        state.sleepTask shouldBe defined
//        //Level2's sleep is ending earlier than Level1's so task should be set for Level2's deadline.
//        state.sleepTask.get._2 shouldBe level2Sleep.sleepDeadline
//
//        //give it sometime and wakeUp call initialised by Level2 will be triggered.
//        sleep(level2Sleep.sleepDeadline.timeLeft + 1.second)
//      }
//    }
//  }
//}
