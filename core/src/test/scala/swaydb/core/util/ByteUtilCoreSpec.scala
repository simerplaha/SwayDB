/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestBase
import swaydb.core.io.file.IO
import swaydb.data.slice.Slice

class ByteUtilCoreSpec extends WordSpec with Matchers {

  "ByteUtil.commonPrefixBytes" should {
    "return common bytes" in {
      val bytes1: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val bytes2: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte, 6.toByte))

      ByteUtilCore.commonPrefixBytes(bytes1, bytes2) shouldBe 4
    }


    "return 0 common bytes when there are no common bytes" in {
      val bytes1: Slice[Byte] = Slice(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
      val bytes2: Slice[Byte] = Slice(Array(5.toByte, 6.toByte, 7.toByte, 8.toByte, 9.toByte, 10.toByte))

      ByteUtilCore.commonPrefixBytes(bytes1, bytes2) shouldBe 0
    }
  }
}
