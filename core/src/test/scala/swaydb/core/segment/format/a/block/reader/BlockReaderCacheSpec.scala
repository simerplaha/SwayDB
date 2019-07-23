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

package swaydb.core.segment.format.a.block.reader

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.slice.Slice
import swaydb.ErrorHandler.CoreError

class BlockReaderCacheSpec extends WordSpec with Matchers {

  "set & read" when {
    "set position is 0" in {
      val state = BlockReaderCache.init(0, Slice[Byte](1.toByte, 2.toByte, 3.toByte))

      BlockReaderCache.read(0, 10, state) shouldBe Slice(1.toByte, 2.toByte, 3.toByte)
      BlockReaderCache.read(1, 10, state) shouldBe Slice(2.toByte, 3.toByte)
      BlockReaderCache.read(2, 10, state) shouldBe Slice(3.toByte)
      BlockReaderCache.read(3, 10, state) shouldBe empty
      BlockReaderCache.read(10, 10, state) shouldBe empty
    }

    "set position is 10" in {
      val state = BlockReaderCache.init(10, Slice[Byte](10.toByte, 11.toByte, 12.toByte))

      (0 to 9) foreach {
        i =>
          BlockReaderCache.read(i, 10, state) shouldBe empty
      }
      BlockReaderCache.read(10, 1, state) shouldBe Slice[Byte](10.toByte)
      BlockReaderCache.read(10, 2, state) shouldBe Slice[Byte](10.toByte, 11.toByte)
      BlockReaderCache.read(10, 3, state) shouldBe Slice[Byte](10.toByte, 11.toByte, 12.toByte)
      BlockReaderCache.read(10, 10, state) shouldBe Slice[Byte](10.toByte, 11.toByte, 12.toByte)

      BlockReaderCache.read(11, 1, state) shouldBe Slice[Byte](11.toByte)
      BlockReaderCache.read(11, 2, state) shouldBe Slice[Byte](11.toByte, 12.toByte)
      BlockReaderCache.read(11, 3, state) shouldBe Slice[Byte](11.toByte, 12.toByte)
      BlockReaderCache.read(11, 10, state) shouldBe Slice[Byte](11.toByte, 12.toByte)

      BlockReaderCache.read(12, 1, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 2, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 3, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 10, state) shouldBe Slice[Byte](12.toByte)

      BlockReaderCache.read(13, 1, state) shouldBe empty
      BlockReaderCache.read(13, 2, state) shouldBe empty
      BlockReaderCache.read(13, 3, state) shouldBe empty
      BlockReaderCache.read(13, 10, state) shouldBe empty
    }
  }
}
