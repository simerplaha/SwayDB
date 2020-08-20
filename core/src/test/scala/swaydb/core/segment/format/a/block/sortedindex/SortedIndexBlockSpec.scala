/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.sortedindex

import org.scalatest.PrivateMethodTester
import swaydb.Compression
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Persistent
import swaydb.core.segment.SegmentIO
import swaydb.core.segment.format.a.block.Block
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.MergeStats
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
import swaydb.data.config.{PrefixCompression, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._

class SortedIndexBlockSpec extends TestBase with PrivateMethodTester {

  implicit val order = KeyOrder.default
  implicit def segmentIO = SegmentIO.random

  "Config" should {
    "disable prefixCompression when normalise defined" in {
      runThis(100.times) {
        val prefixCompression =
          PrefixCompression.Enable(
            keysOnly = randomBoolean(),
            interval = randomPrefixCompressionInterval()
          )

        //test via User created object.
        val configFromUserConfig =
          SortedIndexBlock.Config(
            swaydb.data.config.SortedKeyIndex.Enable(
              prefixCompression = prefixCompression,
              enablePositionIndex = randomBoolean(),
              ioStrategy = _ => randomIOStrategy(),
              compressions = _ => eitherOne(Seq.empty, Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(Int.MinValue)), (LZ4Instance.Fastest, LZ4Decompressor.Fast))))
            )
          )


        runThis(100.times) {
          val index = randomIntMax(1000)
          configFromUserConfig.shouldPrefixCompress(index) shouldBe prefixCompression.interval.shouldCompress(index)
        }

        configFromUserConfig.normaliseIndex shouldBe false

        //internal creation
        val internalConfig =
          SortedIndexBlock.Config(
            ioStrategy = _ => randomIOStrategy(),
            //prefix compression is enabled, so normaliseIndex even though true will set to false in the Config.
            shouldPrefixCompress = prefixCompression.interval.shouldCompress,
            prefixCompressKeysOnly = randomBoolean(),
            enableAccessPositionIndex = randomBoolean(),
            normaliseIndex = true,
            compressions = _ => randomCompressions(),
            enablePrefixCompression = randomBoolean()
          )

        internalConfig.enablePrefixCompression shouldBe false
        internalConfig.normaliseIndex shouldBe true
      }
    }

    "normalise if prefix compression is disabled" in {
      runThis(100.times) {
        val prefixCompression = PrefixCompression.Disable(true)

        //use created config
        val configFromUserConfig =
          SortedIndexBlock.Config(
            swaydb.data.config.SortedKeyIndex.Enable(
              prefixCompression = prefixCompression,
              enablePositionIndex = randomBoolean(),
              ioStrategy = _ => randomIOStrategy(),
              compressions = _ => eitherOne(Seq.empty, Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(Int.MinValue)), (LZ4Instance.Fastest, LZ4Decompressor.Fast))))
            )
          )

        runThis(100.times) {
          val index = randomIntMax(1000)
          configFromUserConfig.shouldPrefixCompress(index) shouldBe false
        }

        configFromUserConfig.normaliseIndex shouldBe true

        //internal creation
        val internalConfig =
          SortedIndexBlock.Config(
            ioStrategy = _ => randomIOStrategy(),
            //prefix compression is disabled, normaliseIndex will always return true.
            shouldPrefixCompress = _ => false,
            prefixCompressKeysOnly = randomBoolean(),
            enableAccessPositionIndex = randomBoolean(),
            normaliseIndex = true,
            compressions = _ => randomCompressions(),
            enablePrefixCompression = true
          )

        internalConfig.enablePrefixCompression shouldBe false
        internalConfig.normaliseIndex shouldBe true
      }
    }
  }

  "init" should {
    "initialise index" in {
      runThis(100.times, log = true) {
        val sortedIndexConfig = SortedIndexBlock.Config.random
        val valuesConfig = ValuesBlock.Config.random
        val keyValues = Benchmark("Generating key-values")(
          MergeStats
            .persistentBuilder(randomizedKeyValues(randomIntMax(1000) max 1))
            .close(sortedIndexConfig.enableAccessPositionIndex)
        )

        val state = SortedIndexBlock.init(keyValues, valuesConfig, sortedIndexConfig)

        val uncompressedBlockInfo = UncompressedBlockInfo(keyValues.maxSortedIndexSize)
        val compressions = sortedIndexConfig.compressions(uncompressedBlockInfo)
        //just check for non-empty. Tests uses random so they result will always be different
        state.compressions(uncompressedBlockInfo).nonEmpty shouldBe compressions.nonEmpty
        state.enableAccessPositionIndex shouldBe sortedIndexConfig.enableAccessPositionIndex
        state.header shouldBe null
        state.compressibleBytes.allocatedSize should be > 1 //should have size more than the header bytes.
      }
    }
  }

  "write, close, readAll & get" in {
    runThis(100.times, log = true) {
      val sortedIndexConfig = SortedIndexBlock.Config.random
      val valuesConfig = ValuesBlock.Config.random
      val stats =
        Benchmark("Generating key-values") {
          MergeStats
            .persistentBuilder(randomizedKeyValues(randomIntMax(1000) max 1))
            .close(sortedIndexConfig.enableAccessPositionIndex)
        }

      val sortedIndex = SortedIndexBlock.init(stats, valuesConfig, sortedIndexConfig)
      val values = ValuesBlock.init(stats, valuesConfig, sortedIndex.builder)

      val keyValues = stats.keyValues

      keyValues foreach {
        keyValue =>
          SortedIndexBlock.write(keyValue, sortedIndex)
          values foreach {
            valuesBlock =>
              ValuesBlock.write(keyValue, valuesBlock)
          }
      }

      //      println(s"sortedIndex.underlyingArraySize: ${sortedIndex.bytes.underlyingArraySize}")
      //      println(s"sortedIndex.size               : ${sortedIndex.bytes.size}")
      //      println(s"sortedIndex.extra              : ${sortedIndex.bytes.underlyingArraySize - sortedIndex.bytes.size}")
      //      println

      val closedSortedIndexBlock = SortedIndexBlock.close(sortedIndex)
      val ref = BlockRefReader[SortedIndexBlock.Offset](closedSortedIndexBlock.blockBytes)
      val header = Block.readHeader(ref.copy())
      val sortedIndexBlock = SortedIndexBlock.read(header)

      val valuesBlockReader: UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
        values map {
          valuesBlock =>
            val closedState = ValuesBlock.close(valuesBlock)
            Block.unblock[ValuesBlock.Offset, ValuesBlock](closedState.blockBytes)
        } orNull

      val expectsCompressions = sortedIndexConfig.compressions(UncompressedBlockInfo(sortedIndex.compressibleBytes.size)).nonEmpty
      sortedIndexBlock.compressionInfo.isDefined shouldBe expectsCompressions
      sortedIndexBlock.enableAccessPositionIndex shouldBe sortedIndexConfig.enableAccessPositionIndex
      if (!sortedIndexConfig.enablePrefixCompression) sortedIndexBlock.hasPrefixCompression shouldBe false
      sortedIndexBlock.headerSize should be > 0
      sortedIndexBlock.offset.start shouldBe header.headerSize
      sortedIndexBlock.normalised shouldBe sortedIndexConfig.normaliseIndex

      if (sortedIndexConfig.normaliseIndex)
        sortedIndex.indexEntries.size shouldBe keyValues.size
      else
        sortedIndex.indexEntries shouldBe empty

      val compressibleSortedIndexReader = Block.unblock(ref.copy())
      val cacheableSortedIndexReader = SortedIndexBlock.unblockedReader(closedSortedIndexBlock)

      Seq(compressibleSortedIndexReader, cacheableSortedIndexReader) foreach {
        sortedIndexReader =>
          //values are not required for this test. Create an empty reader.
          /**
           * TEST - READ ALL
           */
          val readAllKeyValues = SortedIndexBlock.toSlice(keyValues.size, sortedIndexReader, valuesBlockReader)
          keyValues shouldBe readAllKeyValues

          /**
           * TEST - Iterator
           */
          val keyValuesIterator = SortedIndexBlock.iterator(sortedIndexReader, valuesBlockReader)
          keyValuesIterator.toList shouldBe readAllKeyValues

          /**
           * TEST - READ ONE BY ONE
           */
          val searchedKeyValues = ListBuffer.empty[Persistent]
          keyValues.foldLeft(Option.empty[Persistent]) {
            case (previous, keyValue) =>
              val searchedKeyValue = SortedIndexBlock.seekAndMatch(keyValue.key, previous.getOrElse(Persistent.Null), sortedIndexReader, valuesBlockReader).getS
              searchedKeyValue.key shouldBe keyValue.key
              searchedKeyValues += searchedKeyValue
              //randomly set previous
              eitherOne(Some(searchedKeyValue), previous, None)
          }

          keyValues shouldBe searchedKeyValues

          /**
           * SEARCH CONCURRENTLY
           */
          searchedKeyValues.zip(keyValues).par foreach {
            case (persistent, memory) =>
              val searchedPersistent = SortedIndexBlock.seekAndMatch(persistent.key, Persistent.Null, sortedIndexReader.copy(), valuesBlockReader)
              searchedPersistent.getS shouldBe persistent
              searchedPersistent.getS shouldBe memory
          }
      }
    }
  }
}
