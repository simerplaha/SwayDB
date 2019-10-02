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

import java.nio.file.Paths

import org.scalamock.scalatest.MockFactory
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.util.Benchmark
import swaydb.core.{Blocks, TestBase, TestSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IOValues._
import org.scalatest.OptionValues._
import swaydb.core.segment.ReadState
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock

import scala.util.Try

class SegmentSearcherSpec extends TestBase with MockFactory {
  implicit val order = KeyOrder.default
  implicit val limiter = TestSweeper.memorySweeperMax
  implicit def segmentIO = SegmentIO.random

  def randomlySelectHigher(index: Int, keyValues: Slice[Persistent]) =
    eitherOne(None, Try(keyValues(index + (randomIntMax(keyValues.size) max 1))).toOption)

  def randomlySelectLower(index: Int, keyValues: Slice[Persistent]) =
    eitherOne(None, Try(keyValues(index - (randomIntMax(keyValues.size) max 1))).toOption)

  def runSearchTest(keyValues: Slice[Transient], blocks: Blocks): Slice[Persistent] = {
    val persistentKeyValues = Slice.create[Persistent](keyValues.size)

    //TEST - hashIndexSearchOnly == false
    keyValues.foldLeft(Option.empty[Persistent]) {
      case (previous, keyValue) =>
        val got =
          SegmentSearcher.search(
            key = keyValue.key,
            path = Paths.get("test"),
            start = eitherOne(None, previous),
            end = None,
            hashIndexReader = blocks.hashIndexReader,
            binarySearchIndexReader =
              if (blocks.sortedIndexReader.block.hasPrefixCompression || blocks.footer.hasRange)
                blocks.binarySearchIndexReader
              else
                null, //set it to null. BinarySearchIndex is not accessed.
            sortedIndexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader,
            hasRange = keyValues.last.stats.segmentHasRange,
            keyValueCount = keyValues.size,
            readState = ReadState.random
          ).map(_.toPersistent)

        if (got.isEmpty)
          SegmentSearcher.search(
            key = keyValue.key,
            path = Paths.get("test"),
            start = eitherOne(None, previous),
            end = None,
            hashIndexReader = blocks.hashIndexReader,
            binarySearchIndexReader =
              if (blocks.sortedIndexReader.block.hasPrefixCompression || blocks.footer.hasRange)
                blocks.binarySearchIndexReader
              else
                null, //set it to null. BinarySearchIndex is not accessed.
            sortedIndexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader,
            hasRange = keyValues.last.stats.segmentHasRange,
            keyValueCount = keyValues.size,
            readState = ReadState.random
          ).map(_.toPersistent)

        got shouldBe defined
        got shouldBe keyValue

        persistentKeyValues add got.value.toPersistent

        eitherOne(None, got, previous)
    }

    persistentKeyValues should have size keyValues.size

    val zippedPersistentKeyValues = persistentKeyValues.zipWithIndex

    Benchmark("Benchmarking slow test", inlinePrint = true) {
      zippedPersistentKeyValues.foldLeft(Option.empty[Persistent]) {
        case (previous, (keyValue, index)) =>
          val randomStart = randomlySelectLower(index, persistentKeyValues)
          val randomEnd = randomlySelectHigher(index, persistentKeyValues)
          val binarySearchIndexOptional = blocks.binarySearchIndexReader

          randomEnd foreach (end => end.key.readInt() > keyValue.key.readInt())
          val found =
            SegmentSearcher.search(
              key = keyValue.key,
              path = Paths.get("test"),
              start = randomStart,
              end = randomEnd,
              hashIndexReader = None,
              //randomly use binary search index.
              binarySearchIndexReader = binarySearchIndexOptional,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader,
              hasRange = keyValues.last.stats.segmentHasRange,
              keyValueCount = keyValues.size,
              readState = ReadState.random
            ).map(_.toPersistent)

          found.value shouldBe keyValue
          eitherOne(None, found, previous)
      }
    }

    //check keys that do not exist return none.
    val maxKey = keyValues.maxKey().maxKey.readInt()
    ((1 until 100) ++ (maxKey + 1 to maxKey + 100)) foreach {
      key =>
        SegmentSearcher.search(
          key = key,
          path = Paths.get("test"),
          start = None,
          end = None,
          hashIndexReader = blocks.hashIndexReader,
          binarySearchIndexReader = blocks.binarySearchIndexReader,
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReader = Some(ValuesBlock.emptyUnblocked), //give it empty blocks since values are not read.
          hasRange = keyValues.last.stats.segmentHasRange,
          keyValueCount = keyValues.size,
          readState = ReadState.random
        ) shouldBe empty
    }

    persistentKeyValues
  }

  def runSearchHigherTest(keyValues: Slice[Persistent], blocks: Blocks) = {
    //TEST - basic search.

    import order._

    def assertHigher(index: Int, key: Slice[Byte], expectedHigher: Option[Persistent]): Unit = {
      val randomStart = randomlySelectLower(index, keyValues)
      val randomEnd = randomlySelectHigher(index, keyValues)
      val randomBinarySearchIndex = eitherOne(None, blocks.binarySearchIndexReader)

      val got =
        SegmentSearcher.searchHigher(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReader = randomBinarySearchIndex, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReader = blocks.valuesReader,
          keyValueCount = keyValues.size
        ).map(_.toPersistent)

      if (expectedHigher.nonEmpty && got.isEmpty) {
        SegmentSearcher.searchHigher(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReader = randomBinarySearchIndex, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReader = blocks.valuesReader,
          keyValueCount = keyValues.size
        ).map(_.toPersistent)

        println("debug")
      }

      got shouldBe expectedHigher
    }

    keyValues.zipWithIndex.foldRight(Option.empty[Persistent]) {
      case ((keyValue, index), expectedHigher) =>
        keyValue match {
          case response: Persistent.Fixed =>
            assertHigher(index, response.key, expectedHigher)
            Some(response)

          case range: Persistent.Range =>
            (range.fromKey.readInt() until range.toKey.readInt()) foreach {
              rangeKey =>
                assertHigher(index, rangeKey, Some(range))
            }
            //if range's toKey is the same as next fix key-values key then the higher is next next.
            //if next is also a range then the higher is next.
            val higherExpected =
            if (expectedHigher.exists(next => next.isInstanceOf[Persistent.Fixed] && next.key.equiv(range.toKey)))
              Try(keyValues(index + 2)).toOption
            else
              expectedHigher

            assertHigher(index, range.toKey, higherExpected)
            Some(range)
        }
    }
  }

  def runSearchLowerTest(keyValues: Slice[Persistent], blocks: Blocks) = {
    //TEST - basic search.

    def assertLower(index: Int, key: Slice[Byte], expectedLower: Option[Persistent]): Unit = {
      val randomStart = randomlySelectLower(index, keyValues)
      val randomEnd = randomlySelectHigher(index, keyValues)
      //      val randomBinarySearchIndex = eitherOne(None, blocks.binarySearchIndexReader)
      val randomBinarySearchIndex = eitherOne(blocks.binarySearchIndexReader, blocks.binarySearchIndexReader)

      val lower =
        SegmentSearcher.searchLower(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReader = randomBinarySearchIndex, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReader = blocks.valuesReader,
          keyValueCount = keyValues.size
        ).map(_.toPersistent)

      lower shouldBe expectedLower
    }

    keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
      case (expectedLower, (keyValue, index)) =>
        keyValue match {
          case response: Persistent.Fixed =>
            assertLower(index, response.key, expectedLower)
            Some(response)

          case range: Persistent.Range =>
            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              rangeKey =>
                assertLower(index, rangeKey, Some(range))
            }
            assertLower(index, range.fromKey, expectedLower)
            Some(range)
        }
    }
  }

  "all searches" in {
    runThis(100.times, log = true) {
      val _keyValues =
        Benchmark("Generating key-values", inlinePrint = true) {
          randomizedKeyValues(startId = Some(100), count = randomIntMax(200) max 1)
        }

      val fullIndex = randomBoolean()

      val binarySearchCompressions = randomCompressionsOrEmpty()
      val hashIndexCompressions = randomCompressionsOrEmpty()

      val keyValues =
        _keyValues.updateStats(
          binarySearchIndexConfig =
            BinarySearchIndexBlock.Config(
              enabled = true,
              minimumNumberOfKeys = 0,
              fullIndex = fullIndex,
              searchSortedIndexDirectlyIfPossible = randomBoolean(),
              ioStrategy = _ => randomIOAccess(),
              compressions = _ => binarySearchCompressions
            ),
          hashIndexConfig =
            HashIndexBlock.Config(
              maxProbe = 100,
              minimumNumberOfKeys = 0,
              minimumNumberOfHits = 0,
              copyIndex = randomBoolean(),
              allocateSpace = _.requiredSpace * 10,
              ioStrategy = _ => randomIOAccess(),
              compressions = _ => hashIndexCompressions
            ),
          sortedIndexConfig =
            SortedIndexBlock.Config.random.copy(prefixCompressionResetCount = 0)
        )

      val blocks: Blocks = getBlocks(keyValues).get
      blocks.hashIndexReader shouldBe defined
      blocks.hashIndexReader.value.block.miss shouldBe 0
      blocks.hashIndexReader.value.block.hit shouldBe keyValues.last.stats.linkedPosition

      if (binarySearchCompressions.isEmpty) blocks.binarySearchIndexReader.foreach(_.block.compressionInfo shouldBe empty)
      if (hashIndexCompressions.isEmpty) blocks.hashIndexReader.value.block.compressionInfo shouldBe empty

      if (fullIndex && !blocks.sortedIndexReader.block.isNormalisedBinarySearchable)
        blocks.binarySearchIndexReader shouldBe defined

      val persistentKeyValues = Benchmark("Searching key-values")(runSearchTest(keyValues, blocks))
      persistentKeyValues should have size keyValues.size
      Benchmark("Searching higher key-values")(runSearchHigherTest(persistentKeyValues, blocks))
      Benchmark("Searching lower key-values ")(runSearchLowerTest(persistentKeyValues, blocks))
    }
  }
}
