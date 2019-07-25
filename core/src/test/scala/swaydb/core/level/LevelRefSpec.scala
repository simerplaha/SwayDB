/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.level

import java.nio.file.Path

import org.scalamock.scalatest.MockFactory
import swaydb.core.IOValues._
import swaydb.core.TestBase
import swaydb.data.io.Core.Error.Segment.ErrorHandler

import scala.collection.mutable.ListBuffer

class LevelRefSpec extends TestBase with MockFactory {

  "firstPersistentLevel" should {
    "return none" in {
      LevelRef.firstPersistentLevel(None) shouldBe empty
    }

    "return first persistent Level" in {
      val level0 = mock[LevelRef]
      val level1 = mock[NextLevel]

      level0.inMemory _ expects() returning true
      level0.nextLevel _ expects() returning Some(level1)
      level1.inMemory _ expects() returning false

      LevelRef.firstPersistentLevel(Some(level0)) should contain(level1)
    }
  }

  "getLevels" should {
    "return all levels" in {
      val level3 = TestLevel()
      val level2 = TestLevel(nextLevel = Some(level3))
      val level1 = TestLevel(nextLevel = Some(level2))
      val level0 = TestLevelZero(nextLevel = Some(level1))

      val allPaths = Seq(level0, level1, level2, level3).map(_.rootPath)

      LevelRef.getLevels(level0).map(_.rootPath) shouldBe allPaths
      LevelRef.getLevels(level1).map(_.rootPath) shouldBe allPaths.drop(1)
      LevelRef.getLevels(level2).map(_.rootPath) shouldBe allPaths.drop(2)
      LevelRef.getLevels(level3).map(_.rootPath) shouldBe allPaths.drop(3)

      level0.close.runIO
    }
  }

  "foldLeft" when {
    "single level" in {
      val level = TestLevel()
      val paths =
        level.foldLeftLevels(ListBuffer.empty[Path]) {
          case (paths, level) =>
            paths += level.rootPath
        }

      paths should contain only level.rootPath

      level.close.runIO
    }

    "multi level" in {
      val level3 = TestLevel()
      val level2 = TestLevel(nextLevel = Some(level3))
      val level1 = TestLevel(nextLevel = Some(level2))
      val level0 = TestLevelZero(nextLevel = Some(level1))

      def paths(level: LevelRef): Seq[Path] =
        level.foldLeftLevels(ListBuffer.empty[Path]) {
          case (paths, level) =>
            paths += level.rootPath
        }

      val allPaths = Seq(level0, level1, level2, level3).map(_.rootPath)

      paths(level0) shouldBe allPaths
      paths(level1) shouldBe allPaths.drop(1)
      paths(level2) shouldBe allPaths.drop(2)
      paths(level3) shouldBe allPaths.drop(3)

      level0.close.runIO
    }
  }

  "map" when {
    "single level" in {
      val level = TestLevel()
      val paths = level.mapLevels(_.rootPath)

      paths should contain only level.rootPath
      level.close.runIO
    }

    "multi level" in {
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

      level0.close.runIO
    }
  }

  "reversedLevels" in {
    val level3 = TestLevel()
    val level2 = TestLevel(nextLevel = Some(level3))
    val level1 = TestLevel(nextLevel = Some(level2))
    val level0 = TestLevelZero(nextLevel = Some(level1))

    level0.reverseLevels.map(_.rootPath) shouldBe Seq(level3.rootPath, level2.rootPath, level1.rootPath, level0.rootPath)

    level0.close.runIO
  }
}
