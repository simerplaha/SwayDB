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
import swaydb.IO
import swaydb.core.RunThis._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.util.{Benchmark, Bytes}
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.core.CommonAssertions._
import swaydb.IOValues._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SortedIndexBlockSpec extends TestBase with PrivateMethodTester {

  implicit val order = KeyOrder.default
  implicit def memorySweeper = TestLimitQueues.memorySweeper
  implicit def segmentIO = SegmentIO.random

  private def assetEqual(keyValues: Slice[Transient], readKeyValues: Iterable[KeyValue.ReadOnly]) = {
    keyValues.size shouldBe readKeyValues.size
    keyValues.zip(readKeyValues).zipWithIndex foreach {
      case ((transient, persistent: Persistent), index) =>
        persistent.getClass.getSimpleName shouldBe transient.getClass.getSimpleName
//        persistent.key shouldBe transient.key
        persistent.isPrefixCompressed shouldBe transient.isPrefixCompressed
        persistent.accessPosition shouldBe transient.thisKeyValueAccessIndexPosition

        val thisKeyValueRealIndexOffsetFunction = PrivateMethod[Int]('thisKeyValueRealIndexOffset)
        val thisKeyValueRealIndexOffset = transient.stats invokePrivate thisKeyValueRealIndexOffsetFunction()

        if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
          persistent.accessPosition should be > 0
        else
          persistent.accessPosition shouldBe 0 //0 indicates disabled accessIndexPosition

        persistent.indexOffset shouldBe thisKeyValueRealIndexOffset

        if (transient.values.isEmpty)
          persistent.valueOffset shouldBe -1
        else
          persistent.valueOffset shouldBe transient.currentStartValueOffsetPosition

        def expectedNextIndexOffset =
          if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
            thisKeyValueRealIndexOffset + Bytes.sizeOf(transient.indexEntryBytes.size) + transient.indexEntryBytes.size + Bytes.sizeOf(transient.thisKeyValueAccessIndexPosition)
          else
            thisKeyValueRealIndexOffset + Bytes.sizeOf(transient.indexEntryBytes.size) + transient.indexEntryBytes.size

        def expectedNextIndexSize =
          if (keyValues.last.sortedIndexConfig.enableAccessPositionIndex)
            keyValues(index + 1).indexEntryBytes.size + Bytes.sizeOf(keyValues(index + 1).thisKeyValueAccessIndexPosition)
          else
            keyValues(index + 1).indexEntryBytes.size

        if (!keyValues.last.sortedIndexConfig.normaliseIndex)
          if (index < keyValues.size - 1) {
            //if it's not the last
            persistent.nextIndexOffset shouldBe expectedNextIndexOffset
            persistent.nextIndexSize shouldBe expectedNextIndexSize
          } else {
            persistent.nextIndexOffset shouldBe -1
            persistent.nextIndexSize shouldBe 0
          }

        persistent match {
          case response: Persistent.SegmentResponse =>
            response.getOrFetchValue shouldBe transient.getOrFetchValue

          case group: Persistent.Group =>
            group.segment.getAll().get shouldBe transient.asInstanceOf[Transient.Group].keyValues
        }


      case ((_, other), _) =>
        fail(s"Didn't expect type: ${other.getClass.getName}")
    }
  }

  "init" in {
    runThis(100.times, log = true) {
      val normalKeyValues = Benchmark("Generating key-values")(randomizedKeyValues(randomIntMax(1000) max 1))
      val (sortedIndex, keyValues) = SortedIndexBlock.init(normalKeyValues)
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

  "write, close, readAll & get" in {
    runThis(100.times, log = true) {
      val keyValues = Benchmark("Generating key-values")(randomizedKeyValues(2, startId = Some(0), sortedIndexConfig = SortedIndexBlock.Config.random.copy(prefixCompressionResetCount = 0, normaliseIndex = true), addGroups = false, addRanges = false))

      val (sortedIndexBlock, normalisedKeyValues) = SortedIndexBlock.init(keyValues)
      val valuesBlock = ValuesBlock.init(normalisedKeyValues)
      normalisedKeyValues foreach {
        keyValue =>
          SortedIndexBlock.write(keyValue, sortedIndexBlock).get
          valuesBlock foreach {
            valuesBlock =>
              ValuesBlock.write(keyValue, valuesBlock).get
          }
      }

      val closedSortedIndexBlock = SortedIndexBlock.close(sortedIndexBlock).get
      val ref = BlockRefReader[SortedIndexBlock.Offset](closedSortedIndexBlock.bytes)
      val header = Block.readHeader(ref.copy()).get
      val block = SortedIndexBlock.read(header).get

      val valuesBlockReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
        valuesBlock map {
          valuesBlock =>
            val closedState = ValuesBlock.close(valuesBlock).get
            Block.unblock[ValuesBlock.Offset, ValuesBlock](closedState.bytes).get
        }

      val expectsCompressions = normalisedKeyValues.last.sortedIndexConfig.compressions(UncompressedBlockInfo(normalisedKeyValues.last.stats.segmentSortedIndexSize)).nonEmpty
      block.compressionInfo.isDefined shouldBe expectsCompressions
      block.enableAccessPositionIndex shouldBe normalisedKeyValues.last.sortedIndexConfig.enableAccessPositionIndex
      block.hasPrefixCompression shouldBe normalisedKeyValues.last.stats.hasPrefixCompression
      block.headerSize shouldBe SortedIndexBlock.headerSize(expectsCompressions)
      block.offset.start shouldBe header.headerSize

      val sortedIndexReader = Block.unblock(ref.copy()).get
      //values are not required for this test. Create an empty reader.
      /**
       * TEST - READ ALL
       */
      val readAllKeyValues = SortedIndexBlock.readAll(normalisedKeyValues.size, sortedIndexReader, valuesBlockReader).get
      assetEqual(normalisedKeyValues.toSlice, readAllKeyValues)
      /**
       * TEST - READ ONE BY ONE
       */
      val searchedKeyValues = ListBuffer.empty[Persistent]
      keyValues.foldLeft(Option.empty[Persistent]) {
        case (previous, keyValue) =>
          val searchedKeyValue = SortedIndexBlock.search(keyValue.key, previous, sortedIndexReader, valuesBlockReader).get.get
          searchedKeyValue.key shouldBe keyValue.key
          searchedKeyValues += searchedKeyValue
          //randomly set previous
          eitherOne(Some(searchedKeyValue), previous, None)
      }

      assetEqual(normalisedKeyValues.toSlice, searchedKeyValues)

      /**
       * SEARCH CONCURRENTLY
       */
      searchedKeyValues.zip(keyValues).par foreach {
        case (persistent, transient) =>
          val searchedPersistent = SortedIndexBlock.search(persistent.key, None, sortedIndexReader.copy(), valuesBlockReader).get.get
          transient match {
            case transient: Transient.SegmentResponse =>
              searchedPersistent shouldBe persistent
              searchedPersistent shouldBe transient

            case group: Transient.Group =>
              val persistentGroup = searchedPersistent.asInstanceOf[Persistent.Group]
              group.keyValues.par foreach {
                groupKeyValue =>
                  persistentGroup.segment.get(groupKeyValue.key).runRandomIO.get shouldBe groupKeyValue
              }
          }
      }
    }
  }
}
