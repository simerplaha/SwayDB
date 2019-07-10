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
import swaydb.core.data.KeyValue.WriteOnly
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.KeyMatcher
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class BinarySearchIndexSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  def assertSearch(bytes: Slice[Byte],
                   values: Seq[Int],
                   unAlteredIndex: BinarySearchIndex) =
    runThis(10.times) {
      val randomBytes = randomBytesSlice(randomIntMax(100))

      val (adjustedOffset, alteredBytes) =
        eitherOne(
          (unAlteredIndex.offset, bytes),
          (unAlteredIndex.offset, bytes ++ randomBytesSlice(randomIntMax(100))),
          (unAlteredIndex.offset.copy(start = randomBytes.size), randomBytes ++ bytes.close()),
          (unAlteredIndex.offset.copy(start = randomBytes.size), randomBytes ++ bytes ++ randomBytesSlice(randomIntMax(100)))
        )

      val largestValue = values.last

      def matcher(valueToFind: Int)(valueFound: Int): IO[KeyMatcher.Result] =
        IO {
          if (valueToFind == valueFound)
            KeyMatcher.Result.Matched(None, null, None)
          else if (valueToFind < valueFound)
            KeyMatcher.Result.AheadOrNoneOrEnd
          else
            KeyMatcher.Result.BehindFetchNext
        }

      val alteredIndex =
        unAlteredIndex.copy(offset = adjustedOffset)

      values foreach {
        value =>
          BinarySearchIndex.search(
            reader = alteredIndex.createBlockReader(SegmentBlock.createUnblockedReader(alteredBytes).get),
            start = None,
            end = None,
            higherOrLower = None,
            matchValue = matcher(valueToFind = value)
          ).get shouldBe defined
      }

      //check for items not in the index.
      val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)

      notInIndex foreach {
        i =>
          BinarySearchIndex.search(
            reader = alteredIndex.createBlockReader(SegmentBlock.createUnblockedReader(alteredBytes).get),
            start = None,
            end = None,
            higherOrLower = None,
            matchValue = matcher(valueToFind = i)
          ).get shouldBe empty
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
              BinarySearchIndex.State(
                largestValue = largestValue,
                uniqueValuesCount = valuesCount,
                isFullIndex = true,
                minimumNumberOfKeys = 0,
                compressions = eitherOne(Seq.empty, Seq(randomCompression()))
              ).get

            values foreach {
              offset =>
                BinarySearchIndex.write(value = offset, state = state).get
            }

            BinarySearchIndex.close(state).get

            state.writtenValues shouldBe values.size

            state.bytes.isFull shouldBe true

            val index =
              BinarySearchIndex.read(
                offset = BinarySearchIndex.Offset(0, state.bytes.size),
                reader = SegmentBlock.createUnblockedReader(state.bytes).get
              ).get

            index.valuesCount shouldBe state.writtenValues

            //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
            index.bytesPerValue should be <= 4

            assertSearch(
              bytes = state.bytes,
              values = values,
              unAlteredIndex = index
            )
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
          BinarySearchIndex.State(
            largestValue = largestValue,
            uniqueValuesCount = valuesCount,
            isFullIndex = true,
            minimumNumberOfKeys = 0,
            compressions = compression
          ).get

        values foreach {
          value =>
            BinarySearchIndex.write(value = value, state = state).get
        }
        BinarySearchIndex.close(state).get

        state.writtenValues shouldBe values.size

        val index =
          BinarySearchIndex.read(
            offset = BinarySearchIndex.Offset(0, state.bytes.size),
            reader = SegmentBlock.createUnblockedReader(state.bytes).get
          ).get

        index.bytesPerValue shouldBe Bytes.sizeOf(largestValue)

        val headerSize =
          BinarySearchIndex.optimalHeaderSize(
            largestValue = largestValue,
            valuesCount = values.size,
            hasCompression = compression.nonEmpty
          )

        index.headerSize shouldBe headerSize
        index.valuesCount shouldBe values.size

        assertSearch(
          bytes = state.bytes,
          values = values,
          unAlteredIndex = index
        )
      }
    }
  }

  "fully indexed search" should {
    val keyValues =
      randomizedKeyValues(
        count = 1000,
        addPut = true
      ).updateStats(
        binarySearchIndexConfig =
          BinarySearchIndex.Config.random.copy(
            enabled = true,
            minimumNumberOfKeys = 0,
            fullIndex = true
          )
      )

    val segment =
      SegmentBlock.write(keyValues, 0, randomCompressionsOrEmpty()).get

    val blocks =
      readBlocks(segment).get

    blocks.binarySearchIndexReader shouldBe defined
    blocks.binarySearchIndexReader.get.block.isFullIndex shouldBe true

    if (!blocks.sortedIndexReader.block.hasPrefixCompression)
      blocks.binarySearchIndexReader.get.block.valuesCount shouldBe keyValues.size

    "search key-values over binary search" in {

      //get
      keyValues.foldLeft(Option.empty[Persistent]) {
        case (previous, keyValue) =>

          val found =
            BinarySearchIndex.search(
              key = keyValue.minKey,
              start = eitherOne(None, previous),
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get.get

          found shouldBe keyValue
          Some(found)
      }
    }

    "search higher key-values over binary search" in {
      //test higher in reverse order
      keyValues.foldRight(Option.empty[Persistent]) {
        case (keyValue, expectedHigher) =>

          def getHigher(key: Slice[Byte]) =
            BinarySearchIndex.searchHigher(
              key = key,
              start = None,
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get

          keyValue match {
            case fixed: WriteOnly.Fixed =>
              val actualHigher = getHigher(fixed.key)
              actualHigher.map(_.key) shouldBe expectedHigher.map(_.key)

            case range: WriteOnly.Range =>
              (range.fromKey.readInt() until range.toKey.readInt()) foreach {
                key =>
                  val actualHigher = getHigher(key)
                  actualHigher shouldBe range
              }

            case group: WriteOnly.Group =>
              (group.minKey.readInt() until group.maxKey.maxKey.readInt()) foreach {
                key =>
                  val actualHigher = getHigher(key)
                  actualHigher.map(_.key) should contain(group.minKey)
              }
          }

          //get the persistent key-value for the next higher assert.
          SortedIndex.search(
            key = keyValue.minKey,
            startFrom = None,
            indexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader
          ).get
      }
    }

    "search lower key-values over binary search" in {
      //test higher in reverse order
      keyValues.foldLeft(Option.empty[Persistent]) {
        case (expectedLower, keyValue) =>

          def getLower(key: Slice[Byte]) =
            BinarySearchIndex.searchLower(
              key = key,
              start = None,
              end = None,
              binarySearchIndexReader = blocks.binarySearchIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get

          keyValue match {
            case fixed: WriteOnly.Fixed =>
              val actualLower = getLower(fixed.key)
              actualLower.map(_.key) shouldBe expectedLower.map(_.key)

            case range: WriteOnly.Range =>
              (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
                key =>
                  val actualLower = getLower(key)
                  actualLower shouldBe range
              }

            case group: WriteOnly.Group =>
              (group.minKey.readInt() + 1 to group.maxKey.maxKey.readInt()) foreach {
                key =>
                  val actualLower = getLower(key)
                  actualLower.map(_.key) should contain(group.minKey)
              }
          }

          //get the persistent key-value for the next higher assert.
          SortedIndex.search(
            key = keyValue.minKey,
            startFrom = None,
            indexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader
          ).get
      }
    }
  }
}
