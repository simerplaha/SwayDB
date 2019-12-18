///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.block
//
//import org.scalatest.OptionValues._
//import org.scalatest.PrivateMethodTester
//import swaydb.Compression
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.data.Persistent
//import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
//import swaydb.core.segment.merge.MergeKeyValueBuilder
//import swaydb.core.util.Benchmark
//import swaydb.core.{TestBase, TestSweeper}
//import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
//import swaydb.data.config.{PrefixCompression, UncompressedBlockInfo}
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//
//import scala.collection.mutable.ListBuffer
//import scala.collection.parallel.CollectionConverters._
//
//class SortedIndexBlockSpec extends TestBase with PrivateMethodTester {
//
//  implicit val order = KeyOrder.default
//  implicit def memorySweeper = TestSweeper.memorySweeperMax
//  implicit def segmentIO = SegmentIO.random
//
//  "Config" should {
//    "set prefixCompression to zero if normalise defined" in {
//      runThis(100.times) {
//        val prefixCompression = PrefixCompression.Enable(randomIntMax(10) max 1, randomBoolean())
//
//        //test via User created object.
//        val configFromUserConfig =
//          SortedIndexBlock.Config(
//            swaydb.data.config.SortedKeyIndex.Enable(
//              prefixCompression = prefixCompression,
//              enablePositionIndex = randomBoolean(),
//              ioStrategy = _ => randomIOStrategy(),
//              compressions = _ => eitherOne(Seq.empty, Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(Int.MinValue)), (LZ4Instance.Fastest, LZ4Decompressor.Fast))))
//            )
//          )
//
//        configFromUserConfig.prefixCompressionResetCount shouldBe prefixCompression.resetCount
//        configFromUserConfig.normaliseIndex shouldBe false
//
//        //internal creation
//        val internalConfig =
//          SortedIndexBlock.Config(
//            ioStrategy = _ => randomIOStrategy(),
//            //prefix compression is enabled, so normaliseIndex even though true will set to false in the Config.
//            prefixCompressionResetCount = prefixCompression.resetCount,
//            prefixCompressKeysOnly = randomBoolean(),
//            enableAccessPositionIndex = randomBoolean(),
//            normaliseIndex = true,
//            compressions = _ => randomCompressions()
//          )
//
//        internalConfig.prefixCompressionResetCount shouldBe 0
//        internalConfig.normaliseIndex shouldBe true
//      }
//    }
//
//    "normalise if prefix compression is disabled" in {
//      runThis(100.times) {
//        val prefixCompression = PrefixCompression.Disable(true)
//
//        //use created config
//        val configFromUserConfig =
//          SortedIndexBlock.Config(
//            swaydb.data.config.SortedKeyIndex.Enable(
//              prefixCompression = prefixCompression,
//              enablePositionIndex = randomBoolean(),
//              ioStrategy = _ => randomIOStrategy(),
//              compressions = _ => eitherOne(Seq.empty, Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(Int.MinValue)), (LZ4Instance.Fastest, LZ4Decompressor.Fast))))
//            )
//          )
//
//        configFromUserConfig.prefixCompressionResetCount shouldBe 0
//        configFromUserConfig.normaliseIndex shouldBe true
//
//        //internal creation
//        val internalConfig =
//          SortedIndexBlock.Config(
//            ioStrategy = _ => randomIOStrategy(),
//            //prefix compression is disabled, normaliseIndex will always return true.
//            prefixCompressionResetCount = 0 - randomIntMax(10),
//            prefixCompressKeysOnly = randomBoolean(),
//            enableAccessPositionIndex = randomBoolean(),
//            normaliseIndex = true,
//            compressions = _ => randomCompressions()
//          )
//
//        internalConfig.prefixCompressionResetCount shouldBe 0
//        internalConfig.normaliseIndex shouldBe true
//      }
//    }
//  }
//
//  "init" should {
//    "initialise index" in {
//      runThis(100.times, log = true) {
//        val sortedIndexConfig = SortedIndexBlock.Config.random
//        val valuesConfig = ValuesBlock.Config.random
//        val keyValues = Benchmark("Generating key-values")(MergeKeyValueBuilder.persistent(randomizedKeyValues(randomIntMax(1000) max 1)))
//
//        val state = SortedIndexBlock.init(keyValues, valuesConfig, sortedIndexConfig)
//
//        val uncompressedBlockInfo = UncompressedBlockInfo(keyValues.maxSortedIndexSize(sortedIndexConfig.enableAccessPositionIndex))
//        val compressions = sortedIndexConfig.compressions(uncompressedBlockInfo)
//        //just check for non-empty. Tests uses random so they result will always be different
//        state.compressions(uncompressedBlockInfo).nonEmpty shouldBe compressions.nonEmpty
//        state.enableAccessPositionIndex shouldBe sortedIndexConfig.enableAccessPositionIndex
//        state.bytes shouldBe Slice.fill(SortedIndexBlock.headerSize)(0.toByte) //should have header bytes populated
//        state.bytes.allocatedSize should be > (SortedIndexBlock.headerSize + 1) //should have size more than the header bytes.
//      }
//    }
//  }
//
//  "write, close, readAll & get" in {
//    runThis(30.times, log = true) {
//      val sortedIndexConfig = SortedIndexBlock.Config.random
//      val valuesConfig = ValuesBlock.Config.random
//      val keyValues = Benchmark("Generating key-values")(MergeKeyValueBuilder.persistent(randomizedKeyValues(randomIntMax(1000) max 1)))
//
//      val sortedIndex = SortedIndexBlock.init(keyValues, valuesConfig, sortedIndexConfig)
//      val values = ValuesBlock.init(keyValues, valuesConfig, sortedIndex.builder)
//
//      keyValues foreach {
//        keyValue =>
//          SortedIndexBlock.write(keyValue, sortedIndex)
//          values foreach {
//            valuesBlock =>
//              ValuesBlock.write(keyValue, valuesBlock)
//          }
//      }
//
//      //      println(s"sortedIndex.underlyingArraySize: ${sortedIndex.bytes.underlyingArraySize}")
//      //      println(s"sortedIndex.size               : ${sortedIndex.bytes.size}")
//      //      println(s"sortedIndex.extra              : ${sortedIndex.bytes.underlyingArraySize - sortedIndex.bytes.size}")
//      //      println
//
//      val closedSortedIndexBlock = SortedIndexBlock.close(sortedIndex)
//      val ref = BlockRefReader[SortedIndexBlock.Offset](closedSortedIndexBlock.bytes)
//      val header = Block.readHeader(ref.copy())
//      val sortedIndexBlock = SortedIndexBlock.read(header)
//
//      val valuesBlockReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
//        values map {
//          valuesBlock =>
//            val closedState = ValuesBlock.close(valuesBlock)
//            Block.unblock[ValuesBlock.Offset, ValuesBlock](closedState.bytes)
//        }
//
//      val expectsCompressions = sortedIndexConfig.compressions(UncompressedBlockInfo(sortedIndex.bytes.size)).nonEmpty
//      sortedIndexBlock.compressionInfo.isDefined shouldBe expectsCompressions
//      sortedIndexBlock.enableAccessPositionIndex shouldBe sortedIndexConfig.enableAccessPositionIndex
//      if (!sortedIndexConfig.enablePrefixCompression) sortedIndexBlock.hasPrefixCompression shouldBe false
//      sortedIndexBlock.headerSize shouldBe SortedIndexBlock.headerSize
//      sortedIndexBlock.offset.start shouldBe header.headerSize
//      sortedIndexBlock.normalised shouldBe sortedIndexConfig.normaliseIndex
//
//      if (sortedIndexConfig.normaliseIndex)
//        sortedIndex.indexEntries.size shouldBe keyValues.size
//      else
//        sortedIndex.indexEntries shouldBe empty
//
//      val sortedIndexReader = Block.unblock(ref.copy())
//      //values are not required for this test. Create an empty reader.
//      /**
//       * TEST - READ ALL
//       */
//      val readAllKeyValues = SortedIndexBlock.readAll(keyValues.size, sortedIndexReader, valuesBlockReader)
//      keyValues shouldBe readAllKeyValues
//
//      /**
//       * TEST - READ ONE BY ONE
//       */
//      val searchedKeyValues = ListBuffer.empty[Persistent]
//      keyValues.foldLeft(Option.empty[Persistent]) {
//        case (previous, keyValue) =>
//          val searchedKeyValue = SortedIndexBlock.seekAndMatch(keyValue.key, previous, sortedIndexReader, valuesBlockReader).value
//          searchedKeyValue.key shouldBe keyValue.key
//          searchedKeyValues += searchedKeyValue
//          //randomly set previous
//          eitherOne(Some(searchedKeyValue), previous, None)
//      }
//
//      keyValues shouldBe searchedKeyValues
//
//      /**
//       * SEARCH CONCURRENTLY
//       */
//      searchedKeyValues.zip(keyValues).par foreach {
//        case (persistent, memory) =>
//          val searchedPersistent = SortedIndexBlock.seekAndMatch(persistent.key, None, sortedIndexReader.copy(), valuesBlockReader)
//          searchedPersistent.value shouldBe persistent
//          searchedPersistent.value shouldBe memory
//      }
//    }
//  }
//}
