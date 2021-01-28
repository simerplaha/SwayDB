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

package swaydb.core.level.compaction.throttle.behaviour

import swaydb.core.CommonAssertions._
import swaydb.core.level.Level
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.config.MMAP
import swaydb.data.util.OperatingSystem

class BehaviourWakeUp_buildLowerLevels_Spec0 extends BehaviourWakeUp_buildLowerLevels_Spec

class BehaviourWakeUp_buildLowerLevels_Spec1 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourWakeUp_buildLowerLevels_Spec2 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class BehaviourWakeUp_buildLowerLevels_Spec3 extends BehaviourWakeUp_buildLowerLevels_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourWakeUp_buildLowerLevels_Spec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext

  "build lower levels" when {
    "there are 2 levels" when {
      "lower level is the second level" in {
        TestCaseSweeper {
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
        TestCaseSweeper {
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
        TestCaseSweeper {
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
