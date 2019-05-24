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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase

class LevelRefSpec extends TestBase with MockFactory {

  "firstPersistentLevel" should {
    "return none" in {
      LevelRef.firstPersistentLevel(None) shouldBe empty
    }

    "return first persistent Level" in {
      val level0 = mock[Level]
      val level1 = mock[Level]

//      level0.inMemory _ expects() returning true
//      level0.nextLevel _ expects() returning Some(level1)
//      level1.inMemory _ expects() returning false
//
//      LevelRef.firstPersistentLevel(Some(level0)) should contain(level1)
      ???
    }
  }
}
