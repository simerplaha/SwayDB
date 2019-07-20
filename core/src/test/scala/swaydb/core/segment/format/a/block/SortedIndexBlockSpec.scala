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

import org.scalatest.PrivateMethodTester
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader}
import swaydb.core.util.{Benchmark, Bytes}
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SortedIndexBlockSpec extends TestBase with PrivateMethodTester {

  implicit val order = KeyOrder.default

  private def assetEqual(keyValues: Slice[Transient], readKeyValues: Iterable[KeyValue.ReadOnly]) = {
    keyValues.size shouldBe readKeyValues.size
    keyValues.zip(readKeyValues).zipWithIndex.foreach {
      case ((transient, persistent: Persistent), index) =>
        persistent.getClass.getSimpleName shouldBe transient.getClass.getSimpleName
        persistent.key shouldBe transient.key
        persistent.isPrefixCompressed shouldBe transient.isPrefixCompressed
        persistent.accessPosition shouldBe transient.stats.thisKeyValueAccessIndexPosition
        val thisKeyValueRealIndexOffsetFunction = PrivateMethod[Int]('thisKeyValueRealIndexOffset)
        val thisKeyValueRealIndexOffset = transient.stats invokePrivate thisKeyValueRealIndexOffsetFunction()

        if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
          persistent.accessPosition should be > 0
        else
          persistent.accessPosition shouldBe 0 //0 indicates disabled accessIndexPosition
        persistent.indexOffset shouldBe thisKeyValueRealIndexOffset

        def expectedNextIndexOffset =
          if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
            thisKeyValueRealIndexOffset + Bytes.sizeOf(transient.indexEntryBytes.size) + transient.indexEntryBytes.size + Bytes.sizeOf(transient.stats.thisKeyValueAccessIndexPosition)
          else
            thisKeyValueRealIndexOffset + Bytes.sizeOf(transient.indexEntryBytes.size) + transient.indexEntryBytes.size

        def expectedNextIndexSize =
          if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
            keyValues(index + 1).indexEntryBytes.size + Bytes.sizeOf(keyValues(index + 1).stats.thisKeyValueAccessIndexPosition)
          else
            keyValues(index + 1).indexEntryBytes.size

        if (index < keyValues.size - 1) {
          //if it's not the last
          persistent.nextIndexOffset shouldBe expectedNextIndexOffset
          persistent.nextIndexSize shouldBe expectedNextIndexSize
        } else {
          persistent.nextIndexOffset shouldBe -1
          persistent.nextIndexSize shouldBe 0
        }

      case ((_, other), _) =>
        fail(s"Didn't expect type: ${other.getClass.getName}")
    }
  }

  "init" in {
    runThis(10.times, log = true) {
      val keyValues = Benchmark("Generating key-values")(randomizedKeyValues(1000, addPut = true))
      val sortedIndex = SortedIndexBlock.init(keyValues)
      val uncompressedBlockInfo = UncompressedBlockInfo(keyValues.last.stats.segmentSortedIndexSize)
      val compressions = keyValues.last.sortedIndexConfig.compressions(uncompressedBlockInfo)
      //just check for non-empty. Tests uses random so they result will always be different
      sortedIndex.compressions(uncompressedBlockInfo).nonEmpty shouldBe compressions.nonEmpty
      sortedIndex.headerSize shouldBe SortedIndexBlock.headerSize(compressions.nonEmpty)
      sortedIndex.enableAccessPositionIndex shouldBe keyValues.last.sortedIndexConfig.enableAccessPositionIndex
      sortedIndex._bytes shouldBe Slice.fill(sortedIndex.headerSize)(0.toByte) //should have header bytes populated
      sortedIndex._bytes.allocatedSize should be > (SortedIndexBlock.headerSize(false) + 1) //should have size more than the header bytes.
    }
  }

  "write, close, readAll & search" in {
    runThis(10.times) {
      val keyValues = Benchmark("Generating key-values")(randomizedKeyValues(1000, startId = Some(1), addPut = true, addRandomGroups = false, addRandomRanges = false))

      val state = SortedIndexBlock.init(keyValues)
      keyValues foreach {
        keyValue =>
          SortedIndexBlock.write(keyValue, state).get
      }
      val closedState = SortedIndexBlock.close(state).get
      val ref = BlockRefReader[SortedIndexBlock.Offset](closedState.bytes)
      val header = Block.readHeader(ref.copy()).get
      val block = SortedIndexBlock.read(header).get
      val expectsCompressions = keyValues.last.sortedIndexConfig.compressions(UncompressedBlockInfo(keyValues.last.stats.segmentSortedIndexSize)).nonEmpty
      block.compressionInfo.isDefined shouldBe expectsCompressions
      block.enableAccessPositionIndex shouldBe keyValues.last.sortedIndexConfig.enableAccessPositionIndex
      block.hasPrefixCompression shouldBe keyValues.last.stats.hasPrefixCompression
      block.headerSize shouldBe SortedIndexBlock.headerSize(expectsCompressions)
      block.offset.start shouldBe header.headerSize
      val blockedReader = BlockedReader(ref.copy()).get
      val unblockedReader = Block.unblock(blockedReader, randomBoolean()).get
      //values are not required for this test. Create an empty reader.
      val testValuesReader = if (keyValues.last.stats.segmentValuesSize == 0) None else Some(ValuesBlock.emptyUnblocked)
      /**
        * TEST - READ ALL
        */
      val readAllKeyValues = SortedIndexBlock.readAll(keyValues.size, unblockedReader, testValuesReader).get
      assetEqual(keyValues, readAllKeyValues)
      /**
        * TEST - READ ONE BY ONE
        */
      val searchedKeyValues = ListBuffer.empty[Persistent]
      keyValues.foldLeft(Option.empty[Persistent]) {
        case (previous, keyValue) =>
          val searchedKeyValue = SortedIndexBlock.search(keyValue.key, previous, unblockedReader.copy(), testValuesReader).get.get
          searchedKeyValues += searchedKeyValue
          //randomly set previous
          if (randomBoolean())
            Some(searchedKeyValue)
          else
            None
      }
      assetEqual(keyValues, searchedKeyValues)

      /**
        * TEST - searchHigherSeekOne & seekHigher
        */
      val searchedKeyValuesSeekOne = mutable.SortedSet.empty[Persistent](Ordering.by[Persistent, Slice[Byte]](_.key)(order))
      keyValues.foldLeft((Option.empty[Persistent], Option.empty[Persistent])) {
        case ((previousPrevious, previous), keyValue) =>

          val searchedKeyValue =
            previous map {
              previous =>
                //previousPrevious is the key that will require 3 seeks to fetch the next highest.
                //assert that when the next higher key is 2 seeks away it should return none.
                previousPrevious foreach {
                  previousPrevious =>
                    SortedIndexBlock.searchHigherSeekOne(previous.key, previousPrevious, unblockedReader.copy(), testValuesReader).get shouldBe empty
                }
                val searchedKeyValue = SortedIndexBlock.searchHigherSeekOne(previous.key, previous, unblockedReader.copy(), testValuesReader).get.get

                //but normal search should return starting from whichever previous.
                SortedIndexBlock.searchHigher(previous.key, None, unblockedReader.copy(), testValuesReader).get.get.key shouldBe searchedKeyValue.key
                SortedIndexBlock.searchHigher(previous.key, previousPrevious, unblockedReader.copy(), testValuesReader).get.get.key shouldBe searchedKeyValue.key
                SortedIndexBlock.searchHigher(previous.key, Some(previous), unblockedReader.copy(), testValuesReader).get.get.key shouldBe searchedKeyValue.key

                searchedKeyValue
            } getOrElse {
              //if previous is not defined start with a get
              SortedIndexBlock.search(keyValue.key, previous, unblockedReader.copy(), testValuesReader).get.get
            }

          searchedKeyValuesSeekOne += searchedKeyValue
          (previous, Some(searchedKeyValue))
      }
      assetEqual(keyValues, searchedKeyValuesSeekOne)
    }
  }
}
