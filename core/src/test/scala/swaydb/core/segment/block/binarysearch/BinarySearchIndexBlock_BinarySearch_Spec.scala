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
//package swaydb.core.segment.block.binarysearch
//
//import org.scalatest.OptionValues._
//import swaydb.core.CommonAssertions._
//import swaydb.testkit.RunThis._
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.segment.data.Persistent
//import swaydb.core.segment.block.reader.BlockRefReader
//import swaydb.core.segment.block.{Block, KeyMatcher}
//import swaydb.core.util.Bytes
//import swaydb.slice.order.KeyOrder
//import swaydb.slice.Slice
//
//class BinarySearchIndexBlock_BinarySearch_Spec extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = CoreKeyOrders(keyOrder).partialKeyOrder
//
//  "binarySearch" should {
//
//    "write and search" when {
//      def assertSearch(bytes: Slice[Byte],
//                       byteSizePerValue: Int,
//                       values: Seq[Int]) = {
//        val largestValue = values.last
//
//        def matcher(valueToFind: Int, valueFound: Int): KeyMatcher.Result = {
//          //            //println(s"valueToFind: $valueToFind. valueFound: $valueFound")
//          if (valueToFind == valueFound)
//            new KeyMatcher.Result.Matched(None, null, None)
//          else if (valueToFind < valueFound)
//            KeyMatcher.Result.aheadOrNoneOrEndNone
//          else
//            new KeyMatcher.Result.BehindFetchNext(null)
//        }
//
//        def context(valueToFind: Int) =
//          new BinarySearchContext {
//            override val targetKey: Slice[Byte] = Slice.emptyBytes
//            val bytesPerValue: Int = byteSizePerValue
//            val valuesCount: Int = values.size
//            val isFullIndex: Boolean = true
//            val higherOrLower: Option[Boolean] = None
//            val lowestKeyValue: PersistentOptional = None
//            val highestKeyValue: PersistentOptional = None
//
//            def seek(offset: Int) = {
//              val foundValue =
//                if (bytesPerValue == 4)
//                  bytes.take(offset, bytesPerValue).readInt()
//                else
//                  bytes.take(offset, bytesPerValue).readUnsignedInt()
//
//              matcher(valueToFind, foundValue)
//            }
//          }
//
//        values foreach {
//          value =>
//            BinarySearchIndexBlock.binarySearch(context(value)) shouldBe a[BinarySearchGetResult.Some[_]]
//        }
//
//        //check for items not in the index.
//        val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)
//
//        notInIndex foreach {
//          i =>
//            BinarySearchIndexBlock.binarySearch(context(i)) shouldBe a[BinarySearchGetResult.None[_]]
//        }
//      }
//
//      "values have the same byte size" in {
//        runThis(10.times) {
//          Seq(0 to 127, 128 to 300, 16384 to 16384 + 200, Int.MaxValue - 5000 to Int.MaxValue - 1000) foreach {
//            values =>
//              val valuesCount = values.size
//              val largestValue = values.last
//              val state =
//                BinarySearchIndexState(
//                  largestValue = largestValue,
//                  uniqueValuesCount = valuesCount,
//                  isFullIndex = true,
//                  minimumNumberOfKeys = 0,
//                  compressions = _ => Seq.empty
//                ).value
//
//              values foreach {
//                offset =>
//                  BinarySearchIndexBlock.write(indexOffset = offset, state = state)
//              }
//
//              BinarySearchIndexBlock.close(state).value
//
//              state.writtenValues shouldBe values.size
//
//              state.bytes.isFull shouldBe true
//
//              Seq(
//                BlockRefReader[BinarySearchIndexBlockOffset](createRandomFileReader(state.bytes)),
//                BlockRefReader[BinarySearchIndexBlockOffset](state.bytes)
//              ) foreach {
//                reader =>
//                  val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlockOffset](reader))
//
//                  val decompressedBytes = Block.unblock(reader.copy()).readFullBlock()
//
//                  block.valuesCount shouldBe state.writtenValues
//
//                  //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
//                  block.bytesPerValue should be <= 4
//
//                  assertSearch(decompressedBytes, block.bytesPerValue, values)
//              }
//          }
//        }
//      }
//
//      "values have distinct byte size" in {
//        runThis(10.times) {
//          //generate values of uniques sizes.
//          val values = (126 to 130) ++ (16384 - 2 to 16384)
//          val valuesCount = values.size
//          val largestValue = values.last
//          val compression = eitherOne(Seq.empty, Seq(randomCompression()))
//
//          val state =
//            BinarySearchIndexState(
//              largestValue = largestValue,
//              uniqueValuesCount = valuesCount,
//              isFullIndex = true,
//              minimumNumberOfKeys = 0,
//              compressions = _ => compression
//            ).value
//
//          values foreach {
//            value =>
//              BinarySearchIndexBlock.write(indexOffset = value, state = state)
//          }
//          BinarySearchIndexBlock.close(state).value
//
//          state.writtenValues shouldBe values.size
//
//          Seq(
//            BlockRefReader[BinarySearchIndexBlockOffset](createRandomFileReader(state.bytes)),
//            BlockRefReader[BinarySearchIndexBlockOffset](state.bytes)
//          ) foreach {
//            reader =>
//              val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlockOffset](reader))
//
//              val decompressedBytes = Block.unblock(reader.copy()).readFullBlock()
//
//              block.bytesPerValue shouldBe Bytes.sizeOfUnsignedInt(largestValue)
//
//              block.valuesCount shouldBe values.size
//
//              assertSearch(decompressedBytes, block.bytesPerValue, values)
//          }
//        }
//      }
//    }
//  }
//}
