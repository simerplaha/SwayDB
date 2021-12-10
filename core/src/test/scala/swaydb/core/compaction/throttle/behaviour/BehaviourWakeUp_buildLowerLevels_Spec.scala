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

package swaydb.core.compaction.throttle.behaviour

import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.level.{ALevelSpec, Level}
import swaydb.core.{ACoreSpec, TestSweeper, TestExecutionContext, TestForceSave}
import swaydb.utils.OperatingSystem


class BehaviourWakeUp_buildLowerLevels_Spec0 extends BehaviourWakeUp_buildLowerLevels_Spec

class BehaviourWakeUp_buildLowerLevels_Spec1 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourWakeUp_buildLowerLevels_Spec2 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
}

class BehaviourWakeUp_buildLowerLevels_Spec3 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def isMemorySpec = true
}

sealed trait BehaviourWakeUp_buildLowerLevels_Spec extends ALevelSpec {

  implicit val ec = TestExecutionContext.executionContext

  "build lower levels" when {
    "there are 2 levels" when {
      "lower level is the second level" in {
        TestSweeper {
          implicit sweeper =>
            val level = TestLevel(nextLevel = Some(TestLevel()))
            val lowerLevel = level.nextLevel.get.shouldBeInstanceOf[Level]

            val levels = BehaviorWakeUp.buildLowerLevels(level, lowerLevel)
            levels.head shouldBe lowerLevel
            levels.tail shouldBe empty
        }
      }
    }

    "there are 3 levels" when {
      "lower level is the third level" in {
        TestSweeper {
          implicit sweeper =>
            val level = TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel()))))
            val secondLevel = level.nextLevel.get.shouldBeInstanceOf[Level]
            val thirdLevel = secondLevel.nextLevel.get.shouldBeInstanceOf[Level]

            val levels = BehaviorWakeUp.buildLowerLevels(level, thirdLevel)
            levels.head shouldBe secondLevel
            levels.tail should contain only thirdLevel
        }
      }

      "lower level is the second level" in {
        TestSweeper {
          implicit sweeper =>
            val level = TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel()))))
            val secondLevel = level.nextLevel.get.shouldBeInstanceOf[Level]
            //third level is ignored because it comes after the last level (secondLevel)
            val thirdLevel = secondLevel.nextLevel.get.shouldBeInstanceOf[Level]

            val levels = BehaviorWakeUp.buildLowerLevels(level, secondLevel)
            levels.head shouldBe secondLevel
            levels.tail shouldBe empty
        }
      }
    }
  }
}
