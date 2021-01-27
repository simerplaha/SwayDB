/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.sweeper

import org.scalamock.scalatest.MockFactory
import swaydb.core.sweeper.FileSweeper._
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}

import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentSkipListSet
import scala.collection.mutable.ListBuffer
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.data.Memory
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.data.config.ActorConfig
import TestCaseSweeper._
import swaydb.data.RunThis._

import scala.concurrent.duration._

class FileSweeperSpec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty


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
          val skipList = new ConcurrentSkipListSet[Command.Close](actorQueueOrder)

          def addCloseFiles(path: String) = {
            val closeFile1 = Command.CloseFileItem(file(path))
            skipList add closeFile1

            val closeFiles2 = Command.CloseFiles.of(Seq(file(path)))
            skipList add closeFiles2
            List(closeFile1, closeFiles2)
          }

          val closeFileCommands = ListBuffer.empty[Command.CloseFile]
          closeFileCommands ++= addCloseFiles("file1")

          val pause1 = Command.Pause(Seq(TestLevel()))
          skipList add pause1

          closeFileCommands ++= addCloseFiles("file2")

          val resume1 = Command.Resume(Seq(TestLevel()))
          skipList add resume1

          closeFileCommands ++= addCloseFiles("file3")

          val pause2 = Command.Pause(Seq(TestLevel()))
          skipList add pause2

          closeFileCommands ++= addCloseFiles("file4")

          val resume2 = Command.Resume(Seq(TestLevel()))
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
          //set stashCapacity to 0 so no messages are cached.
          implicit val fileSweeper = FileSweeper(0, ActorConfig.Timer("FileSweeper Test Timer", 0.second, TestExecutionContext.executionContext)).sweep()

          val level = TestLevel(segmentConfig = SegmentBlock.Config.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 1.byte, cacheBlocksOnCreate = false))
          fileSweeper.send(Command.Pause(Seq(level)))

          level.put(Seq(Memory.put(1), Memory.put(2), Memory.put(3), Memory.put(4)))

          level.segments() should have size 4

          level.segments().foreach(_.isOpen shouldBe false)
          level.segments().foreach(_.mightContainKey(1, ThreadReadState.random)) //random request to open the Segment
          level.segments().foreach(_.isOpen shouldBe true)
          sleep(1.second)
          level.segments().foreach(_.isOpen shouldBe true)

          fileSweeper.send(Command.Resume(Seq(level)))

          //after resume all files are eventually closed
          eventual(10.seconds) {
            level.segments().foreach(_.isOpen shouldBe false)
          }
      }
    }
  }
}
