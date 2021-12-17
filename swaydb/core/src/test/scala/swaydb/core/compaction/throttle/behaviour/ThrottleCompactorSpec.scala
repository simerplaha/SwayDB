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
////
////package swaydb.core.compaction.throttle
////
////import org.scalamock.scalatest.MockFactory
////import org.scalatest.OptionValues._
////import swaydb.Actor
////import swaydb.core.CommonAssertions._
////import swaydb.core.CoreTestSweeper._
////import swaydb.core.TestData._
////import swaydb.core.level.Level
////import swaydb.core.compaction.{Compaction, Compactor}
////import swaydb.core._
////import swaydb.testkit.RunThis._
////import swaydb.config.compaction.CompactionExecutionContext
////import swaydb.config.MMAP
////import swaydb.slice.order.{KeyOrder, TimeOrder}
////import swaydb.slice.Slice
////import swaydb.config.util.OperatingSystem
////
////import scala.collection.mutable
////import scala.concurrent.Promise
////import scala.concurrent.duration._
////
////class ThrottleCompactorSpec0 extends ThrottleCompactorSpec
////
////class ThrottleCompactorSpec1 extends ThrottleCompactorSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////}
////
////class ThrottleCompactorSpec2 extends ThrottleCompactorSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
////  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
////  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
////}
////
////class ThrottleCompactorSpec3 extends ThrottleCompactorSpec {
////  override def inMemoryStorage = true
////}
////
////sealed trait ThrottleCompactorSpec extends TestBase  {
////
////  val keyValueCount = 1000
////
////  implicit val ec = TestExecutionContext.executionContext
////  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
////  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
////  implicit val timer = TestTimer.Empty
////
////  implicit val compactionOrdering = ThrottleLevelOrdering
////
////  implicit val compaction = ThrottleCompaction
////
////  override def deleteFiles = false
////
////  "createActors" should {
////    "build compaction hierarchy" when {
////      "there are two Levels and one new ExecutionContext" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val nextLevel = TestLevel()
////            val zero = TestLevelZero(nextLevel = Some(nextLevel))
////
////            val parallelism = randomParallelMerge()
////
////            val actors =
////              ThrottleCompactor.createActors(
////                List(zero, nextLevel),
////                List(
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, TestExecutionContext.executionContext, parallelism, randomIntMax(Byte.MaxValue).max(1)),
////                  CompactionExecutionContext.Shared
////                )
////              ).get
////
////            actors should have size 1
////            val actor = actors.head
////
////            actor.state.await.compactionStates shouldBe empty
////            actor.state.await.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
////            actor.state.await.child shouldBe empty
////            actor.state.await.parallelMerge shouldBe parallelism
////        }
////      }
////
////      "there are two Levels and two new ExecutionContext" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val nextLevel = TestLevel()
////            val zero = TestLevelZero(nextLevel = Some(nextLevel))
////
////            val parallelism = randomParallelMerge()
////
////            val actors =
////              ThrottleCompactor.createActors(
////                List(zero, nextLevel),
////                List(
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, TestExecutionContext.executionContext, parallelism, randomIntMax(Byte.MaxValue).max(1)),
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, TestExecutionContext.executionContext, parallelism, randomIntMax(Byte.MaxValue).max(1))
////                )
////              ).get
////
////            actors should have size 2
////            val actor = actors.head
////
////            actor.state.await.compactionStates shouldBe empty
////            actor.state.await.levels.map(_.rootPath) should contain only zero.rootPath
////            actor.state.await.child shouldBe defined
////            actor.state.await.parallelMerge shouldBe parallelism
////
////            val childActor = actor.state.await.child.get.state.await
////            childActor.child shouldBe empty
////            childActor.levels.map(_.rootPath) should contain only nextLevel.rootPath
////            childActor.parallelMerge shouldBe parallelism
////        }
////      }
////
////      "there are three Levels and one new ExecutionContext" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val nextLevel2 = TestLevel()
////            val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
////            val zero = TestLevelZero(nextLevel = Some(nextLevel))
////
////            val parallelism = randomParallelMerge()
////
////            val actors =
////              ThrottleCompactor.createActors(
////                List(zero, nextLevel, nextLevel2),
////                List(
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, parallelism, randomIntMax(Byte.MaxValue).max(1)),
////                  CompactionExecutionContext.Shared,
////                  CompactionExecutionContext.Shared
////                )
////              ).get
////
////            actors should have size 1
////            val actor = actors.head
////
////            actor.state.await.compactionStates shouldBe empty
////            actor.state.await.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath, nextLevel2.rootPath)
////            actor.state.await.child shouldBe empty
////            actor.state.await.parallelMerge shouldBe parallelism
////        }
////      }
////
////      "there are three Levels and two new ExecutionContext" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val nextLevel2 = TestLevel()
////            val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
////            val zero = TestLevelZero(nextLevel = Some(nextLevel))
////
////            val parallelism1 = randomParallelMerge()
////            val parallelism2 = randomParallelMerge()
////
////            val actors =
////              ThrottleCompactor.createActors(
////                List(zero, nextLevel, nextLevel2),
////                List(
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, parallelism1, randomIntMax(Byte.MaxValue).max(1)),
////                  CompactionExecutionContext.Shared,
////                  CompactionExecutionContext.Create(TestExecutionContext.executionContext, parallelism2, randomIntMax(Byte.MaxValue).max(1))
////                )
////              ).get
////
////            actors should have size 2
////            val actor = actors.head
////
////            actor.state.await.compactionStates shouldBe empty
////            actor.state.await.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
////            actor.state.await.child shouldBe defined
////            actor.state.await.parallelMerge shouldBe parallelism1
////
////            val childActor = actor.state.await.child.get.state.await
////            childActor.child shouldBe empty
////            childActor.levels.map(_.rootPath) should contain only nextLevel2.rootPath
////            childActor.parallelMerge shouldBe parallelism2
////        }
////      }
////    }
////  }
////
////  "scheduleNextWakeUp" should {
////    def createTestLevel()(implicit sweeper: CoreTestSweeper): (Level, Level, ThrottleState) = {
////      val nextLevel = TestLevel()
////      val level = TestLevel(nextLevel = Some(nextLevel))
////
////      val testState =
////        ThrottleState(
////          levels = Slice(level, nextLevel),
////          parallelMerge = randomParallelMerge(),
////          //        ordering = CompactionOrdering.ordering(_ => ThrottleLevelState.Sleeping(1.day.fromNow, 0)),
////          resetCompactionPriorityAtInterval = randomIntMax(Byte.MaxValue).max(1),
////          child = None,
////          executionContext = TestExecutionContext.executionContext,
////          compactionStates = mutable.Map.empty
////        )
////
////      (level, nextLevel, testState)
////    }
////
////    "not trigger wakeUp" when {
////      "level states were empty" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////
////            val (level, nextLevel, testState) = createTestLevel()
////
////            val compactor = mock[Compactor]
////
////            val actor =
////              Actor.wire[Compactor](
////                name = "test",
////                impl = compactor,
////                state = testState
////              ).sweep()
////
////            ThrottleCompactor.scheduleNextWakeUp(
////              state = testState,
////              self = actor
////            )
////
////            sleep(5.seconds)
////        }
////      }
////
////      "level states were non-empty but level's state is unchanged and has scheduled task" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////
////            val (level, nextLevel, testState) = createTestLevel()
////
////            val compactor = mock[Compactor]
////            val state =
////              testState.copy(
////                compactionStates =
////                  mutable.Map(
////                    level -> ThrottleLevelState.Sleeping(5.seconds.fromNow, level.stateId),
////                    nextLevel -> ThrottleLevelState.Sleeping(5.seconds.fromNow, nextLevel.stateId)
////                  )
////              )
////
////            state.sleepTask = Some(null)
////
////            val actor =
////              Actor.wire[Compactor](
////                name = "test",
////                impl = compactor,
////                state = state
////              ).sweep()
////
////            state.sleepTask shouldBe defined
////
////            ThrottleCompactor.scheduleNextWakeUp(
////              state = state,
////              self = actor
////            )
////
////            sleep(3.seconds)
////        }
////      }
////
////      "level states were non-empty but level's state is unchanged and task is undefined" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////
////            val (level, nextLevel, testState) = createTestLevel()
////
////            val compactor = mock[Compactor]
////
////            val state =
////              testState.copy(
////                compactionStates =
////                  mutable.Map(
////                    level -> ThrottleLevelState.Sleeping(4.seconds.fromNow, level.stateId),
////                    nextLevel -> ThrottleLevelState.Sleeping(eitherOne(7.seconds.fromNow, 4.seconds.fromNow), nextLevel.stateId)
////                  )
////              )
////
////            val actor =
////              Actor.wire[Compactor](
////                name = "",
////                impl = compactor,
////                state = state
////              ).sweep()
////
////            state.sleepTask shouldBe empty
////
////            ThrottleCompactor.scheduleNextWakeUp(
////              state = state,
////              self = actor
////            )
////
////            eventual(state.sleepTask shouldBe defined)
////
////            sleep(2.seconds)
////
////            //eventually
////            compactor.wakeUp _ expects(*, *, *) onCall {
////              (throttle, copyForward, actor) =>
////                copyForward shouldBe false
////                throttle shouldBe state
////                actor shouldBe actor
////            }
////
////            sleep(3.seconds)
////        }
////      }
////    }
////
////    "trigger wakeUp" when {
////      "one of level states is awaiting pull and successfully received read" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            import sweeper._
////
////            val (level, nextLevel, testState) = createTestLevel()
////
////            //create IO.Later that is busy
////            val promise = Promise[Unit]()
////
////            val awaitingPull = ThrottleLevelState.AwaitingPull(promise, 1.minute.fromNow, 0)
////            awaitingPull.listenerInvoked shouldBe false
////            //set the state to be awaiting pull
////            val state =
////              testState.copy(
////                compactionStates =
////                  mutable.Map(
////                    level -> awaitingPull
////                  )
////              )
////            //mock the compaction that should expect a wakeUp call
////            val compactor = mock[Compactor]
////            compactor.wakeUp _ expects(*, *, *) onCall {
////              (callState, doCopy, _) =>
////                callState shouldBe state
////                doCopy shouldBe false
////            }
////
////            //initialise Compactor with the mocked class
////            val actor =
////              Actor.wire[Compactor](
////                name = "test",
////                impl = compactor,
////                state = state
////              )
////
////            ThrottleCompactor.scheduleNextWakeUp(
////              state = state,
////              self = actor
////            )
////            //after calling scheduleNextWakeUp listener should be initialised.
////            //this ensures that multiple wakeUp callbacks do not value registered for the same pull.
////            awaitingPull.listenerInitialised shouldBe true
////
////            //free the reserve and compaction should expect a message.
////            scheduler.future(1.second)(promise.success(()))
////
////            eventual(3.seconds) {
////              //eventually is set to be ready.
////              awaitingPull.listenerInvoked shouldBe true
////              //next sleep task is initialised & it's the await's timeout.
////              state.sleepTask.value._2 shouldBe awaitingPull.timeout
////            }
////        }
////      }
////
////      "one of level states is awaiting pull and other Level's sleep is shorter" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////
////            val (level, nextLevel, testState) = createTestLevel()
////
////            val promise = Promise[Unit]()
////
////            val level1AwaitingPull = ThrottleLevelState.AwaitingPull(promise, 1.minute.fromNow, 0)
////            level1AwaitingPull.listenerInvoked shouldBe false
////
////            //level 2's sleep is shorter than level1's awaitPull timeout sleep.
////            val level2Sleep = ThrottleLevelState.Sleeping(5.seconds.fromNow, 0)
////            //set the state to be awaiting pull
////            val state =
////              testState.copy(
////                compactionStates =
////                  mutable.Map(
////                    level -> level1AwaitingPull,
////                    nextLevel -> level2Sleep
////                  )
////              )
////            //mock the compaction that should expect a wakeUp call
////            val compactor = mock[Compactor]
////            compactor.wakeUp _ expects(*, *, *) onCall {
////              (callState, doCopy, _) =>
////                callState shouldBe state
////                doCopy shouldBe false
////            }
////
////            //initialise Compactor with the mocked class
////            val actor =
////              Actor.wire[Compactor](
////                name = "test",
////                impl = compactor,
////                state = state
////              )
////
////            ThrottleCompactor.scheduleNextWakeUp(
////              state = state,
////              self = actor
////            )
////            //a callback for awaiting pull should always be initialised.
////            level1AwaitingPull.listenerInitialised shouldBe true
////            state.sleepTask shouldBe defined
////            //Level2's sleep is ending earlier than Level1's so task should be set for Level2's deadline.
////            state.sleepTask.get._2 shouldBe level2Sleep.sleepDeadline
////
////            //give it sometime and wakeUp call initialised by Level2 will be triggered.
////            sleep(level2Sleep.sleepDeadline.timeLeft + 1.second)
////        }
////      }
////    }
////  }
////
////  "doWakeUp" should {
////
////
////    "run compaction and postCompaction" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////
////          val nextLevel = TestLevel()
////          val level = TestLevel(nextLevel = Some(nextLevel))
////
////          val testState =
////            ThrottleState(
////              levels = Slice(level, nextLevel),
////              parallelMerge = randomParallelMerge(),
////              //        ordering = CompactionOrdering.ordering(_ => ThrottleLevelState.Sleeping(1.day.fromNow, 0)),
////              resetCompactionPriorityAtInterval = randomIntMax(Byte.MaxValue).max(1),
////              child = None,
////              executionContext = TestExecutionContext.executionContext,
////              compactionStates = mutable.Map.empty
////            )
////
////          implicit val compaction = mock[Compaction[ThrottleState]]
////
////          val parentCompactor = mock[Compactor]
////          val childCompactor = mock[Compactor]
////
////          val copyForward = randomBoolean()
////
////          val childActor =
////            Actor.wire[Compactor](
////              name = "test",
////              impl = childCompactor,
////              state = testState
////            ).sweep()
////
////          val state: ThrottleState =
////            testState.copy(compactionStates = mutable.Map.empty, child = Some(childActor))
////
////          val actor =
////            Actor.wire[Compactor](
////              name = "test",
////              impl = parentCompactor,
////              state = state
////            ).sweep()
////
////          //parent gets a compaction call
////          compaction.run _ expects(*, *) onCall {
////            (throttle, copy) =>
////              throttle shouldBe state
////              copy shouldBe copyForward
////              ()
////          }
////
////          //child get a compaction call.
////          childCompactor.wakeUp _ expects(*, *, *) onCall {
////            (state, copy, actor) =>
////              copy shouldBe false
////              ()
////          }
////
////          ThrottleCompactor.doWakeUp(
////            state = state,
////            forwardCopyOnAllLevels = copyForward,
////            self = actor
////          )
////
////          sleep(5.seconds)
////      }
////    }
////  }
////}
