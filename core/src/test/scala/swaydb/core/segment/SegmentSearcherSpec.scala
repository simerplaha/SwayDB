/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */
package swaydb.core.segment

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Persistent, PersistentOption}
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.SegmentSearcher
import swaydb.core.util.Benchmark
import swaydb.core.{SegmentBlocks, TestBase, TestCaseSweeper}
import swaydb.testkit.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Try

class SegmentSearcherSpec extends TestBase with MockFactory {

  implicit val order = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(order))
  implicit def segmentIO = SegmentReadIO.random

  def randomlySelectHigher(index: Int, keyValues: Slice[Persistent]): PersistentOption =
    eitherOne(Persistent.Null, Try(keyValues(index + (randomIntMax(keyValues.size) max 1))).toOption.getOrElse(Persistent.Null))

  def randomlySelectLower(index: Int, keyValues: Slice[Persistent]): PersistentOption =
    eitherOne(Persistent.Null, Try(keyValues(index - (randomIntMax(keyValues.size) max 1))).toOption.getOrElse(Persistent.Null))

  def runSearchTest(keyValues: Slice[Memory], blocks: SegmentBlocks): Slice[Persistent] = {
    val persistentKeyValues = Slice.of[Persistent](keyValues.size)

    //TEST - hashIndexSearchOnly == false
    keyValues.foldLeft(Persistent.Null: PersistentOption) {
      case (previous, keyValue) =>
        val got =
          SegmentSearcher.searchRandom(
            key = keyValue.key,
            start = eitherOne(Persistent.Null, previous),
            end = Persistent.Null,
            hashIndexReaderOrNull = blocks.hashIndexReader.orNull,
            binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
            sortedIndexReader = blocks.sortedIndexReader,
            valuesReaderOrNull = blocks.valuesReader.orNull,
            hasRange = blocks.footer.hasRange,
            keyValueCount = keyValues.size
          )

        if (got.isNoneS)
          SegmentSearcher.searchRandom(
            key = keyValue.key,
            start = eitherOne(Persistent.Null, previous),
            end = Persistent.Null,
            hashIndexReaderOrNull = blocks.hashIndexReader.orNull,
            binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
            sortedIndexReader = blocks.sortedIndexReader,
            valuesReaderOrNull = blocks.valuesReader.orNull,
            hasRange = blocks.footer.hasRange,
            keyValueCount = keyValues.size
          )

        got.toOptionS shouldBe defined
        got.toOptionS.value shouldBe keyValue

        persistentKeyValues add got.getS

        eitherOne(Persistent.Null, got, previous)
    }

    persistentKeyValues should have size keyValues.size

    val zippedPersistentKeyValues = persistentKeyValues.zipWithIndex

    Benchmark("Benchmarking slow test", inlinePrint = true) {
      zippedPersistentKeyValues.foldLeft(Persistent.Null: PersistentOption) {
        case (previous, (keyValue, index)) =>
          val randomStart = randomlySelectLower(index, persistentKeyValues)
          val randomEnd = randomlySelectHigher(index, persistentKeyValues)
          val binarySearchIndexOptional = blocks.binarySearchIndexReader

          randomEnd foreachS (end => end.key.readInt() > keyValue.key.readInt())
          val found =
            SegmentSearcher.searchRandom(
              key = keyValue.key,
              start = randomStart,
              end = randomEnd,
              hashIndexReaderOrNull = null,
              //randomly use binary search index.
              binarySearchIndexReaderOrNull = binarySearchIndexOptional.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull,
              hasRange = blocks.footer.hasRange,
              keyValueCount = keyValues.size
            )

          found.getS shouldBe keyValue
          eitherOne(Persistent.Null, found, previous)
      }
    }

    //check keys that do not exist return none.
    val maxKey = keyValues.maxKey().maxKey.readInt()
    ((1 until 100) ++ (maxKey + 1 to maxKey + 100)) foreach {
      key =>
        SegmentSearcher.searchRandom(
          key = key,
          start = Persistent.Null,
          end = Persistent.Null,
          hashIndexReaderOrNull = blocks.hashIndexReader.orNull,
          binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReaderOrNull = ValuesBlock.emptyUnblocked, //give it empty blocks since values are not read.
          hasRange = blocks.footer.hasRange,
          keyValueCount = keyValues.size
        ).toOptionS shouldBe empty
    }

    persistentKeyValues
  }

  def runSearchHigherTest(keyValues: Slice[Persistent], blocks: SegmentBlocks) = {
    //TEST - basic search.

    import order._

    def assertHigher(index: Int, key: Slice[Byte], expectedHigher: PersistentOption): Unit = {
      val randomStart = randomlySelectLower(index, keyValues)
      val randomEnd = randomlySelectHigher(index, keyValues)
      val randomBinarySearchIndex = eitherOne(None, blocks.binarySearchIndexReader)

      val got =
        SegmentSearcher.searchHigherRandomly(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReaderOrNull = randomBinarySearchIndex.orNull, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReaderOrNull = blocks.valuesReader.orNull,
          keyValueCount = keyValues.size
        )

      if (expectedHigher.isSomeS && got.isNoneS) {
        SegmentSearcher.searchHigherRandomly(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReaderOrNull = randomBinarySearchIndex.orNull, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReaderOrNull = blocks.valuesReader.orNull,
          keyValueCount = keyValues.size
        )

        println("debug")
      }

      got shouldBe expectedHigher
    }

    keyValues.zipWithIndex.foldRight(Persistent.Null: PersistentOption) {
      case ((keyValue, index), expectedHigher) =>
        keyValue match {
          case response: Persistent.Fixed =>
            assertHigher(index, response.key, expectedHigher)
            response

          case range: Persistent.Range =>
            (range.fromKey.readInt() until range.toKey.readInt()) foreach {
              rangeKey =>
                assertHigher(index, rangeKey, range)
            }
            //if range's toKey is the same as next fix key-values key then the higher is next next.
            //if next is also a range then the higher is next.
            val higherExpected: PersistentOption =
            if (expectedHigher.existsS(next => next.isInstanceOf[Persistent.Fixed] && next.key.equiv(range.toKey)))
              Try(keyValues(index + 2)).toOption.getOrElse(Persistent.Null)
            else
              expectedHigher

            assertHigher(index, range.toKey, higherExpected)
            range
        }
    }
  }

  def runSearchLowerTest(keyValues: Slice[Persistent], blocks: SegmentBlocks) = {
    //TEST - basic search.

    def assertLower(index: Int, key: Slice[Byte], expectedLower: PersistentOption): Unit = {
      val randomStart = randomlySelectLower(index, keyValues)
      val randomEnd = randomlySelectHigher(index, keyValues)
      val randomBinarySearchIndex = eitherOne(None, blocks.binarySearchIndexReader)

      val lower =
        SegmentSearcher.searchLower(
          key = key,
          //randomly give it start and end indexes.
          start = randomStart,
          end = randomEnd,
          binarySearchIndexReaderOrNull = randomBinarySearchIndex.orNull, //set it to null. BinarySearchIndex is not accessed.
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReaderOrNull = blocks.valuesReader.orNull,
          keyValueCount = keyValues.size
        )

      lower shouldBe expectedLower
    }

    keyValues.zipWithIndex.foldLeft(Persistent.Null: PersistentOption) {
      case (expectedLower, (keyValue, index)) =>
        keyValue match {
          case response: Persistent.Fixed =>
            assertLower(index, response.key, expectedLower)
            response

          case range: Persistent.Range =>
            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              rangeKey =>
                assertLower(index, rangeKey, range)
            }
            assertLower(index, range.fromKey, expectedLower)
            range
        }
    }
  }

  "all searches" in {
    runThis(100.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val keyValues =
            Benchmark("Generating key-values", inlinePrint = true) {
              randomizedKeyValues(startId = Some(100), count = randomIntMax(100) max 1)
            }

          val segments: Slice[SegmentBlocks] =
            Benchmark(s"Creating Segment for ${keyValues.size}") {
              getBlocks(
                keyValues = keyValues,
                segmentConfig = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue, mmap = mmapSegments)
              ).get
            }

          segments should have size 1
          val blocks = segments.head

          val persistentKeyValues = Benchmark("Searching key-values")(runSearchTest(keyValues, blocks))
          persistentKeyValues should have size keyValues.size
          Benchmark("Searching higher key-values")(runSearchHigherTest(persistentKeyValues, blocks))
          Benchmark("Searching lower key-values ")(runSearchLowerTest(persistentKeyValues, blocks))
      }
    }
  }
}
