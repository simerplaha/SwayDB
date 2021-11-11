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

package swaydb.core.sweeper

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data.Memory
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.sweeper.FileSweeper._
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.{ActorConfig, _}

import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentSkipListSet
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._

class FileSweeperSpec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  private def file(filePath: String): FileSweeperItem =
    new FileSweeperItem {
      override def path: Path = Paths.get(filePath)
      override def delete(): Unit = ()
      override def close(): Unit = ()
      override def isOpen: Boolean = true
    }

  "queueOrder" should {
    "prioritise PauseResume messages" in {
      TestCaseSweeper {
        implicit sweeper =>
          val skipList = new ConcurrentSkipListSet[FileSweeper.Command.Close](actorQueueOrder)

          def addCloseFiles(path: String) = {
            val closeFile1 = FileSweeper.Command.CloseFileItem(file(path))
            skipList add closeFile1

            val closeFiles2 = FileSweeper.Command.CloseFiles.of(Seq(file(path)))
            skipList add closeFiles2
            List(closeFile1, closeFiles2)
          }

          val closeFileCommands = ListBuffer.empty[FileSweeper.Command.CloseFile]
          closeFileCommands ++= addCloseFiles("file1")

          val pause1 = FileSweeper.Command.Pause(Seq(TestLevel()))
          skipList add pause1

          closeFileCommands ++= addCloseFiles("file2")

          val resume1 = FileSweeper.Command.Resume(Seq(TestLevel()))
          skipList add resume1

          closeFileCommands ++= addCloseFiles("file3")

          val pause2 = FileSweeper.Command.Pause(Seq(TestLevel()))
          skipList add pause2

          closeFileCommands ++= addCloseFiles("file4")

          val resume2 = FileSweeper.Command.Resume(Seq(TestLevel()))
          skipList add resume2

          closeFileCommands ++= addCloseFiles("file5")

          skipList.pollFirst() shouldBe pause1
          skipList.pollFirst() shouldBe resume1
          skipList.pollFirst() shouldBe pause2
          skipList.pollFirst() shouldBe resume2

          closeFileCommands should have size 10

          closeFileCommands foreach {
            closeFileCommand =>
              skipList.pollFirst() shouldBe closeFileCommand
          }
      }
    }
  }

  "pause and resume level" in {
    runThis(5.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //set stashCapacity to 0 so no messages are cached.
          implicit val fileSweeper = FileSweeper(0, ActorConfig.Timer("FileSweeper Test Timer", 0.second, TestExecutionContext.executionContext)).sweep()

          val level = TestLevel(segmentConfig = SegmentBlockConfig.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 1.byte, cacheBlocksOnCreate = false))
          fileSweeper.send(FileSweeper.Command.Pause(Seq(level)))

          level.put(Seq(Memory.put(1), Memory.put(2), Memory.put(3), Memory.put(4))) shouldBe IO.unit

          level.segments() should have size 4

          level.segments().foreach(_.isOpen shouldBe false)
          level.segments().foreach(_.mightContainKey(1, ThreadReadState.random)) //random request to open the Segment
          level.segments().foreach(_.isOpen shouldBe true)
          sleep(1.second)
          level.segments().foreach(_.isOpen shouldBe true)

          fileSweeper.send(FileSweeper.Command.Resume(Seq(level)))

          //after resume all files are eventually closed
          eventual(10.seconds) {
            level.segments().foreach(_.isOpen shouldBe false)
          }
      }
    }
  }

  "stress" in {
    runThis(5.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          /**
           * Objective: Concurrently read all Segments in the Level that are being
           * closed by [[FileSweeper]] and expect all reads to succeed without
           * any closed file exceptions when the Level is paused and resumed.
           */

          //set stashCapacity to 0 so files are closed immediately.
          implicit val fileSweeper = FileSweeper(0, ActorConfig.Timer("FileSweeper Test Timer", 0.second, TestExecutionContext.executionContext)).sweep()

          val keyValuesCount = randomIntMax(100) max 1

          val keyValues = randomPutKeyValues(keyValuesCount)

          //Level with 1.byte segmentSize so each key-values have it's own Segment
          val level = TestLevel(segmentConfig = SegmentBlockConfig.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 1.byte, cacheBlocksOnCreate = false))

          fileSweeper.send(FileSweeper.Command.Pause(Seq(level))) //pause closing
          level.put(keyValues) shouldBe IO.unit //write the Segment
          fileSweeper.send(FileSweeper.Command.Resume(Seq(level))) //resume closing

          //1 segment per key-value
          level.segmentsCount() shouldBe keyValues.size

          val levelSegments = level.segments().toSlice.zipWithIndex

          //open all files
          runThis(1000.times, log = true) {
            //invoke pause
            fileSweeper.send(FileSweeper.Command.Pause(Seq(level)))

            //concurrently read all Segments and none should fail due to FileClosedException
            //because the Level is paused. No randomIO is used so no retry required.
            levelSegments.par foreach {
              case (segment, index) =>
                val keyValue = keyValues(index)
                segment.get(keyValue.key, ThreadReadState.random).getUnsafe shouldBe keyValue
            }
            //resume
            fileSweeper.send(FileSweeper.Command.Resume(Seq(level)))
          }

          //eventually all files are closed
          eventual(10.seconds) {
            level.segments().foreach(_.isOpen shouldBe false)
          }
      }
    }
  }
}
