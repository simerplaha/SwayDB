/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment.ref.search

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.TestData._

import java.nio.file.Paths

class ThreadReadStateSpec extends AnyWordSpec with Matchers {

  "it" should {
    "return Null" when {
      "not states exist" in {
        val state = ThreadReadState.random

        (1 to 100) foreach {
          _ =>
            state.getSegmentState(Paths.get(randomString)) shouldBe SegmentReadState.Null
        }
      }

      "states exists but queries states do not exist" in {
        val state = ThreadReadState.limitHashMap(100, 100)

        val keys =
          (1 to 100) map {
            _ =>
              val key = Paths.get(randomString)
              state.setSegmentState(key, null)
              key
          }

        keys foreach {
          key =>
            state.getSegmentState(key) shouldBe SegmentReadState.Null
        }
      }
    }
  }
}
