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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import swaydb.core.{TestBase, TestCaseSweeper}

import java.nio.file.Path
import scala.collection.mutable.ListBuffer

class LevelRefSpec extends TestBase with MockFactory {

  "firstPersistentLevel" should {
    "return none" in {
      LevelRef.firstPersistentLevel(None) shouldBe empty
    }

    "return first persistent Level" in {
      val level0 = mock[LevelRef]
      val level1 = mock[NextLevel]

      (level0.inMemory _).expects() returning true
      (level0.nextLevel _).expects() returning Some(level1)
      (level1.inMemory _).expects() returning false

      LevelRef.firstPersistentLevel(Some(level0)) should contain(level1)
    }
  }

  "getLevels" should {
    "return all levels" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level3 = TestLevel()
          val level2 = TestLevel(nextLevel = Some(level3))
          val level1 = TestLevel(nextLevel = Some(level2))
          val level0 = TestLevelZero(nextLevel = Some(level1))

          val allPaths = Seq(level0, level1, level2, level3).map(_.rootPath)

          LevelRef.getLevels(level0).map(_.rootPath) shouldBe allPaths
          LevelRef.getLevels(level1).map(_.rootPath) shouldBe allPaths.drop(1)
          LevelRef.getLevels(level2).map(_.rootPath) shouldBe allPaths.drop(2)
          LevelRef.getLevels(level3).map(_.rootPath) shouldBe allPaths.drop(3)
      }
    }
  }

  "foldLeft" when {
    "single level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          val paths =
            level.foldLeftLevels(ListBuffer.empty[Path]) {
              case (paths, level) =>
                paths += level.rootPath
            }

          paths should contain only level.rootPath
      }
    }

    "multi level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level3 = TestLevel()
          val level2 = TestLevel(nextLevel = Some(level3))
          val level1 = TestLevel(nextLevel = Some(level2))
          val level0 = TestLevelZero(nextLevel = Some(level1))

          def paths(level: LevelRef): Iterable[Path] =
            level.foldLeftLevels(ListBuffer.empty[Path]) {
              case (paths, level) =>
                paths += level.rootPath
            }

          val allPaths = Seq(level0, level1, level2, level3).map(_.rootPath)

          paths(level0) shouldBe allPaths
          paths(level1) shouldBe allPaths.drop(1)
          paths(level2) shouldBe allPaths.drop(2)
          paths(level3) shouldBe allPaths.drop(3)
      }
    }
  }

  "map" when {
    "single level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          val paths = level.mapLevels(_.rootPath)

          paths should contain only level.rootPath
      }
    }

    "multi level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level3 = TestLevel()
          val level2 = TestLevel(nextLevel = Some(level3))
          val level1 = TestLevel(nextLevel = Some(level2))
          val level0 = TestLevelZero(nextLevel = Some(level1))

          def paths(level: LevelRef) = level.mapLevels(_.rootPath)

          val allPaths = Seq(level0, level1, level2, level3).map(_.rootPath)

          paths(level0) shouldBe allPaths
          paths(level1) shouldBe allPaths.drop(1)
          paths(level2) shouldBe allPaths.drop(2)
          paths(level3) shouldBe allPaths.drop(3)
      }
    }
  }

  "reversedLevels" in {
    TestCaseSweeper {
      implicit sweeper =>
        val level3 = TestLevel()
        val level2 = TestLevel(nextLevel = Some(level3))
        val level1 = TestLevel(nextLevel = Some(level2))
        val level0 = TestLevelZero(nextLevel = Some(level1))

        level0.reverseLevels.map(_.rootPath) shouldBe Seq(level3.rootPath, level2.rootPath, level1.rootPath, level0.rootPath)
    }
  }
}
