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
//package swaydb.core.compaction.throttle.behaviour
//
//import swaydb.IO
//import swaydb.config.MMAP
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestData._
//import swaydb.core._
//import swaydb.core.compaction.task.CompactionTask
//import swaydb.core.level.{ALevelSpec, Level}
//import swaydb.core.log.ALogSpec
//import swaydb.core.segment.Segment
//import swaydb.core.segment.data.Memory
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//
//import scala.collection.SortedSet
//import swaydb.testkit.TestKit._
//
//class BehaviourCompactionTask_compactSegments_2Levels_Spec0 extends BehaviourCompactionTask_compactSegments_2Levels_Spec
//
//class BehaviourCompactionTask_compactSegments_2Levels_Spec1 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class BehaviourCompactionTask_compactSegments_2Levels_Spec2 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class BehaviourCompactionTask_compactSegments_2Levels_Spec3 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
//  override def isMemorySpec = true
//}
//
//sealed trait BehaviourCompactionTask_compactSegments_2Levels_Spec extends AnyWordSpec with ALogSpec {
//
//  implicit val timer = TestTimer.Empty
//  implicit val keyOrder = KeyOrder.default
//  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
//  implicit val ec = TestExecutionContext.executionContext
//
//  "compactSegments" when {
//    "there are 2 levels" should {
//      "write data to lower level" when {
//        "target Level is empty" in {
//          CoreTestSweeper {
//            implicit sweeper =>
//              import sweeper._
//
//              //LEVEL1 - [1], [2]
//              //LEVEL2 - EMPTY
//              val sourceLevel = TestLevel()
//              val targetLevel = TestLevel()
//
//              val segments = SortedSet(TestSegment(Slice(Memory.put(1))), TestSegment(Slice(Memory.put(2))))
//              val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//              sourceLevel.putSegments(segments) shouldBe IO.unit
//
//              sourceLevel.isEmpty shouldBe false
//              targetLevel.isEmpty shouldBe true
//
//              assertReads(keyValues, sourceLevel)
//              assertEmpty(keyValues, targetLevel)
//
//              val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
//              BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit
//
//              sourceLevel.isEmpty shouldBe true
//              targetLevel.isEmpty shouldBe false
//
//              assertEmpty(keyValues, sourceLevel)
//              assertReads(keyValues, targetLevel)
//
//              if (isPersistent) {
//                val reopenSource = sourceLevel.reopen
//                val reopenTarget = targetLevel.reopen
//
//                assertEmpty(keyValues, reopenSource)
//                assertReads(keyValues, reopenTarget)
//              }
//          }
//        }
//
//        "target Level is non empty" in {
//          CoreTestSweeper {
//            implicit sweeper =>
//              import sweeper._
//
//              //LEVEL1 - [1], [2]
//              //LEVEL2 - {[1], [2]} || {1, 2}
//              val sourceLevel = TestLevel()
//              val targetLevel = TestLevel()
//
//              val segments = SortedSet(TestSegment(Slice(Memory.put(1))), TestSegment(Slice(Memory.put(2))))
//              val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//              sourceLevel.putSegments(segments) shouldBe IO.unit
//              //write data to Level as a single Segment or multiple Segments
//              eitherOne(
//                targetLevel.putSegments(segments) shouldBe IO.unit,
//                targetLevel.put(keyValues.mapToSlice(_.toMemory())) shouldBe IO.unit
//              )
//
//              sourceLevel.isEmpty shouldBe false
//              targetLevel.isEmpty shouldBe false
//
//              assertReads(keyValues, sourceLevel)
//              assertReads(keyValues, targetLevel)
//
//              val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
//              BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit
//
//              sourceLevel.isEmpty shouldBe true
//              targetLevel.isEmpty shouldBe false
//
//              assertEmpty(keyValues, sourceLevel)
//              assertReads(keyValues, targetLevel)
//
//              if (isPersistent) {
//                val reopenSource = sourceLevel.reopen
//                val reopenTarget = targetLevel.reopen
//
//                assertEmpty(keyValues, reopenSource)
//                assertReads(keyValues, reopenTarget)
//              }
//          }
//        }
//      }
//
//      "expire data" when {
//        "target Level is empty" in {
//          runThis(10.times, log = true) {
//            CoreTestSweeper {
//              implicit sweeper =>
//                import sweeper._
//
//                //LEVEL1 - [1], [2]
//                //LEVEL2 - EMPTY
//                val sourceLevel = TestLevel()
//                val targetLevel = TestLevel()
//
//                val segments =
//                  SortedSet(
//                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(2))),
//                    TestSegment(Slice(randomUpdateKeyValue(3)))
//                  )
//
//                val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//                sourceLevel.putSegments(segments) shouldBe IO.unit
//
//                sourceLevel.isEmpty shouldBe false
//                targetLevel.isEmpty shouldBe true
//
//                assertReads(keyValues, sourceLevel)
//                assertEmpty(keyValues, targetLevel)
//
//                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
//                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit
//
//                sourceLevel.isEmpty shouldBe true
//                targetLevel.isEmpty shouldBe true
//
//                assertEmpty(keyValues, sourceLevel)
//                assertEmpty(keyValues, targetLevel)
//
//                if (isPersistent) {
//                  val reopenSource = sourceLevel.reopen
//                  val reopenTarget = targetLevel.reopen
//
//                  assertEmpty(keyValues, reopenSource)
//                  assertEmpty(keyValues, reopenTarget)
//                }
//            }
//          }
//        }
//
//        "target Level is non empty" in {
//          runThis(20.times, log = true) {
//            CoreTestSweeper {
//              implicit sweeper =>
//                import sweeper._
//
//                val sourceLevel = TestLevel()
//                val targetLevel = TestLevel()
//
//                //all source key-values are expired or update
//                val sourceLevelSegment =
//                  SortedSet(
//                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(2))),
//                    TestSegment(Slice(randomUpdateKeyValue(3, deadline = randomExpiredDeadlineOption())))
//                  )
//
//                val sourceLevelKeyValues = sourceLevelSegment.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//                //all target key-values are expired or update
//                val targetLevelSegment =
//                  SortedSet(
//                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(3))),
//                    TestSegment(Slice(randomUpdateKeyValue(4), randomUpdateKeyValue(5)))
//                  )
//
//                val targetLevelKeyValues = targetLevelSegment.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//                sourceLevel.putSegments(sourceLevelSegment) shouldBe IO.unit
//                //write data to Level as a single Segment or multiple Segments
//                eitherOne(
//                  targetLevel.putSegments(targetLevelSegment) shouldBe IO.unit,
//                  targetLevel.put(targetLevelKeyValues.mapToSlice(_.toMemory())) shouldBe IO.unit
//                )
//
//                unexpiredPuts(sourceLevelKeyValues) should have size 0
//                unexpiredPuts(targetLevelKeyValues) should have size 0
//
//                sourceLevel.isEmpty shouldBe false
//                targetLevel.isEmpty shouldBe false
//
//                assertReads(sourceLevelKeyValues, sourceLevel)
//                assertReads(targetLevelKeyValues, targetLevel)
//
//                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, sourceLevelSegment)))
//                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit
//
//                sourceLevel.isEmpty shouldBe true
//                targetLevel.isEmpty shouldBe true
//
//                if (isPersistent) {
//                  val reopenSource = sourceLevel.reopen
//                  val reopenTarget = targetLevel.reopen
//
//                  reopenSource.isEmpty shouldBe true
//                  reopenTarget.isEmpty shouldBe true
//                }
//            }
//          }
//        }
//      }
//
//      "delete uncommitted Segment" when {
//        "failure" in {
//          runThis(5.times, log = true) {
//            CoreTestSweeper {
//              implicit sweeper =>
//                import sweeper._
//
//                //LEVEL1 - [1], [2]
//                //LEVEL2 - EMPTY
//                val sourceLevel = TestLevel()
//                val targetLevel = TestLevel()
//
//                val segments =
//                  SortedSet(
//                    TestSegment(Slice(Memory.put(1))),
//                    TestSegment(Slice(Memory.put(2)))
//                  )
//
//                val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice
//
//                sourceLevel.putSegments(segments) shouldBe IO.unit
//
//                //assert that nothing changes in the Level.
//                def assertNoChange(sourceLevel: Level, targetLevel: Level) = {
//                  sourceLevel.isEmpty shouldBe false
//                  targetLevel.isEmpty shouldBe true
//
//                  if (isPersistent) {
//                    targetLevel.segmentFilesOnDisk() shouldBe empty
//                    segments.foreach(_.existsOnDiskOrMemory() shouldBe true)
//                  } else {
//                    //for memory we delete the last Segment so expect others to exist
//                    segments.dropRight(1).foreach(_.existsOnDiskOrMemory() shouldBe true)
//                  }
//
//                  assertReads(keyValues, sourceLevel)
//                  assertEmpty(keyValues, targetLevel)
//                }
//
//                assertNoChange(sourceLevel, targetLevel)
//
//                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
//
//                val existingSegment =
//                  if (isMemorySpec) { //if it's a memory test delete the last Segment to test failure
//                    val segment = segments.last
//                    segment.delete()
//                    segment
//                  } else {
//                    //for persistent expect
//                    TestSegment(path = targetLevel.rootPath.resolve(s"${targetLevel.segmentIDGenerator.currentId() + 1}.seg"))
//                  }
//
//                //Create a Segment file in the Level's folder giving it the Level's next SegmentId so that it fails
//                //compaction because the fileName is already taken
//                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitFailureInf shouldBe a[Exception]
//                if (isPersistent) existingSegment.delete() //delete the Segment so that assertion can assert for empty Level
//
//                //state remains unchanged
//                assertNoChange(sourceLevel, targetLevel)
//
//                if (isPersistent) {
//                  val reopenSource = sourceLevel.reopen
//                  val reopenTarget = targetLevel.reopen
//                  assertNoChange(reopenSource, reopenTarget)
//                }
//            }
//          }
//        }
//      }
//    }
//  }
//}
