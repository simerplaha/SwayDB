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

package swaydb.core.segment.format.a.block

import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchEntryFormat
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

class BinarySearchEntryFormatSpec extends TestBase {

  "ReferenceIndex" when {
    "non zero" should {
      "calculate bytes required" in {
        BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
          largestIndexOffset = 10,
          largestMergedKeySize = 10
        ) shouldBe 1

        BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
          largestIndexOffset = Int.MaxValue,
          largestMergedKeySize = 10
        ) shouldBe ByteSizeOf.varInt
      }

      "write and read only the indexOffset" in {

        Seq(0, 10000000, Int.MaxValue) foreach {
          indexOffset =>
            val bytesRequired =
              BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
                largestIndexOffset = indexOffset,
                largestMergedKeySize = 10
              )

            val bytes = Slice.create[Byte](bytesRequired)

            BinarySearchEntryFormat.Reference.write(
              indexOffset = indexOffset,
              mergedKey = randomBytesSlice(),
              keyType = 1,
              bytes = bytes
            )

            bytes.size shouldBe Bytes.sizeOfUnsignedInt(indexOffset)
            bytes.readUnsignedInt() shouldBe indexOffset
        }
      }
    }
  }
}
