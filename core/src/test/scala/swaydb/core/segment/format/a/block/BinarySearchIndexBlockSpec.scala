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

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class BinarySearchIndexBlockSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  def assertSearch(bytes: Slice[Byte],
                   values: Seq[Int]) =
    runThis(10.times) {
      val largestValue = values.last

      def matcher(valueToFind: Int)(valueFound: Int): IO[KeyMatcher.Result] =
        IO {
          if (valueToFind == valueFound)
            KeyMatcher.Result.Matched(None, null, None)
          else if (valueToFind < valueFound)
            KeyMatcher.Result.AheadOrNoneOrEnd
          else
            KeyMatcher.Result.BehindFetchNext(null)
        }

      values foreach {
        value =>
          BinarySearchIndexBlock.search(
            reader = Block.unblock[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](bytes).get,
            start = None,
            end = None,
            higherOrLower = None,
            matchValue = matcher(valueToFind = value)
          ).get shouldBe a[SearchResult.Some[Int]]
      }

      //check for items not in the index.
      val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)

      notInIndex foreach {
        i =>
          BinarySearchIndexBlock.search(
            reader = Block.unblock[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](bytes).get,
            start = None,
            end = None,
            higherOrLower = None,
            matchValue = matcher(valueToFind = i)
          ).get shouldBe a[SearchResult.None[Int]]
      }
    }

  "write full index" when {
    "all values have the same size" in {
      runThis(10.times) {
        Seq(0 to 127, 128 to 300, 16384 to 16384 + 200, Int.MaxValue - 5000 to Int.MaxValue - 1000) foreach {
          values =>
            val valuesCount = values.size
            val largestValue = values.last
            val state =
              BinarySearchIndexBlock.State(
                largestValue = largestValue,
                uniqueValuesCount = valuesCount,
                isFullIndex = true,
                minimumNumberOfKeys = 0,
                compressions = _ => Seq.empty
              ).get

            values foreach {
              offset =>
                BinarySearchIndexBlock.write(value = offset, state = state).get
            }

            BinarySearchIndexBlock.close(state).get

            state.writtenValues shouldBe values.size

            state.bytes.isFull shouldBe true

            val index = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](BlockRefReader(state.bytes)).get).get

            index.valuesCount shouldBe state.writtenValues

            //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
            index.bytesPerValue should be <= 4

            assertSearch(state.bytes, values)
        }
      }
    }

    "all values have unique size" in {
      runThis(10.times) {
        val values = (126 to 130) ++ (16384 - 2 to 16384)
        val valuesCount = values.size
        val largestValue = values.last
        val compression = eitherOne(Seq.empty, Seq(randomCompression()))
        val state =
          BinarySearchIndexBlock.State(
            largestValue = largestValue,
            uniqueValuesCount = valuesCount,
            isFullIndex = true,
            minimumNumberOfKeys = 0,
            compressions = _ => compression
          ).get

        values foreach {
          value =>
            BinarySearchIndexBlock.write(value = value, state = state).get
        }
        BinarySearchIndexBlock.close(state).get

        state.writtenValues shouldBe values.size

        val index = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](BlockRefReader(state.bytes)).get).get

        index.bytesPerValue shouldBe Bytes.sizeOf(largestValue)

        index.valuesCount shouldBe values.size

        assertSearch(bytes = state.bytes, values = values)
      }
    }
  }

  "fully indexed search" should {
    val keyValues =
      randomizedKeyValues(
        count = 1000,
        startId = Some(1),
        addRandomRanges = false,
        addRandomGroups = false,
        addPut = true
      ).updateStats(
        binarySearchIndexConfig =
          BinarySearchIndexBlock.Config.random.copy(
            enabled = true,
            minimumNumberOfKeys = 0,
            fullIndex = true
          ),
        sortedIndexConfig =
          SortedIndexBlock.Config.random.copy(
            prefixCompressionResetCount = 0
          )
      )

    val blocks = getBlocks(keyValues).get

    blocks.binarySearchIndexReader shouldBe defined
    blocks.binarySearchIndexReader.get.block.isFullIndex shouldBe true

    if (!blocks.sortedIndexReader.block.hasPrefixCompression)
      blocks.binarySearchIndexReader.get.block.valuesCount shouldBe keyValues.size

    "search key-values over binary search" in {
      //get
      keyValues.foldLeft(Option.empty[Persistent]) {
        case (previous, keyValue) =>

          val startFrom = eitherOne(None, previous)

          println(s"Find: ${keyValue.minKey.readInt()}")
          val found =
            BinarySearchIndexBlock.search(
              key = keyValue.minKey,
              start = startFrom,
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get match {
              case SearchResult.None(lower) =>
                lower

              case SearchResult.Some(lower, value) =>
                //if startFrom is given, search either return a more nearest lowest of the passed in lowest.
                if (startFrom.isDefined) lower shouldBe defined

                lower foreach {
                  lower =>
                    //lower should always be less than keyValue
                    lower.key.readInt() should be < keyValue.key.readInt()
                    startFrom foreach {
                      startFrom =>
                        //if startFrom is defined lower key should be >= startFrom's key
                        lower.key.readInt() should be >= startFrom.key.readInt()
                    }
                }
                value.key shouldBe keyValue.minKey
                Some(value)
            }

          found shouldBe keyValue
          found
      }
    }

    "search higher key-values over binary search" in {
      //test higher in reverse order
      keyValues.foldRight(Option.empty[Persistent]) {
        case (keyValue, expectedHigher) =>

          def getHigher(key: Slice[Byte]) =
            BinarySearchIndexBlock.searchHigher(
              key = key,
              start = None,
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get

          keyValue match {
            case fixed: Transient.Fixed =>
              getHigher(fixed.key) match {
                case SearchResult.None(lower) =>
                  lower.get.key.readInt() should be < fixed.key.readInt()
                  expectedHigher shouldBe empty

                case SearchResult.Some(lower, actualHigher) =>
                  lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                  actualHigher.key shouldBe expectedHigher.get.key
              }

            case range: Transient.Range =>
              (range.fromKey.readInt() until range.toKey.readInt()) foreach {
                key =>
                  getHigher(key) match {
                    case SearchResult.None(lower) =>
                      lower.get.key.readInt() should be < key
                      expectedHigher shouldBe empty

                    case SearchResult.Some(lower, actualHigher) =>
                      lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                      actualHigher shouldBe range
                  }
              }
            //
            case group: Transient.Group =>
              (group.minKey.readInt() until group.maxKey.maxKey.readInt()) foreach {
                key =>
                  getHigher(key) match {
                    case SearchResult.None(lower) =>
                      lower.get.key.readInt() should be < key
                      expectedHigher shouldBe empty

                    case SearchResult.Some(lower, actualHigher) =>
                      lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                      actualHigher.key shouldBe group.minKey
                  }
              }
          }

          //get the persistent key-value for the next higher assert.
          SortedIndexBlock.search(
            key = keyValue.minKey,
            startFrom = None,
            indexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader
          ).get
      }
    }

    "search lower key-values over binary search" in {
      //test higher in reverse order
      keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
        case (expectedLower, (keyValue, index)) =>

          def getLower(key: Slice[Byte]) =
            BinarySearchIndexBlock.searchLower(
              key = key,
              start = None,
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get

          keyValue match {
            case fixed: Transient.Fixed =>
              getLower(fixed.key) match {
                case SearchResult.None(lower) =>
                  lower shouldBe empty

                case SearchResult.Some(lower, actualLower) =>
                  lower shouldBe empty
                  actualLower.key shouldBe expectedLower.get.key
              }

            case range: Transient.Range =>
              (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
                key =>
                  getLower(key) match {
                    case SearchResult.None(lower) =>
                      lower shouldBe empty

                    case SearchResult.Some(lower, actualLower) =>
                      lower shouldBe empty
                      actualLower shouldBe range
                  }
              }

            case group: Transient.Group =>
              (group.minKey.readInt() + 1 to group.maxKey.maxKey.readInt()) foreach {
                key =>
                  getLower(key) match {
                    case SearchResult.None(lower) =>
                      lower shouldBe empty

                    case SearchResult.Some(lower, actualLower) =>
                      lower shouldBe empty
                      actualLower.key shouldBe group.minKey
                  }
              }
          }

          //get the persistent key-value for the next higher assert.
          SortedIndexBlock.search(
            key = keyValue.minKey,
            startFrom = None,
            indexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader
          ).get
      }
    }
  }
}
