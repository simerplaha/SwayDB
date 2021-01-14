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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.core.TestData._
import swaydb.data.RunThis._
import swaydb.data.slice.Slice

class CRC32Spec extends AnyFlatSpec with Matchers {

  it should "apply CRC on bytes" in {
    runThis(100.times) {
      val bytes = randomBytesSlice(randomIntMax(100) + 1)
      CRC32.forBytes(bytes) should be >= 1L
    }
  }

  it should "return 0 for empty bytes" in {
    CRC32.forBytes(Slice.emptyBytes) shouldBe 0L
  }
}
