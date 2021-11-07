///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.segment.block.binarysearch
//
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.util.Bytes
//import swaydb.data.slice.Slice
//import swaydb.utils.ByteSizeOf
//
//class BinarySearchEntryFormatSpec extends TestBase {
//
//  "ReferenceIndex" when {
//    "non zero" should {
//      "calculate bytes required" in {
//        BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
//          largestIndexOffset = 10,
//          largestMergedKeySize = 10
//        ) shouldBe 1
//
//        BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
//          largestIndexOffset = Int.MaxValue,
//          largestMergedKeySize = 10
//        ) shouldBe ByteSizeOf.varInt
//      }
//
//      "write and read only the indexOffset" in {
//
//        Seq(0, 10000000, Int.MaxValue) foreach {
//          indexOffset =>
//            val bytesRequired =
//              BinarySearchEntryFormat.Reference.bytesToAllocatePerEntry(
//                largestIndexOffset = indexOffset,
//                largestMergedKeySize = 10
//              )
//
//            val bytes = Slice.of[Byte](bytesRequired)
//
//            BinarySearchEntryFormat.Reference.write(
//              indexOffset = indexOffset,
//              mergedKey = randomBytesSlice(),
//              keyType = 1,
//              bytes = bytes
//            )
//
//            bytes.size shouldBe Bytes.sizeOfUnsignedInt(indexOffset)
//            bytes.readUnsignedInt() shouldBe indexOffset
//        }
//      }
//    }
//  }
//}
