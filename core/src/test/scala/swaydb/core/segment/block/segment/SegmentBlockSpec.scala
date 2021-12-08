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
//package swaydb.core.segment.block.segment
//
//import org.scalatest.OptionValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestData._
//import swaydb.core.file.reader.Reader
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
//import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
//import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
//import swaydb.core.segment.block.values.ValuesBlockConfig
//import swaydb.core.segment.data._
//import swaydb.core.segment.data.merge.stats.MergeStats
//import swaydb.core.segment.io.SegmentReadIO
//import swaydb.core.{ACoreSpec, TestCaseSweeper, TestExecutionContext, TestTimer}
//import swaydb.core.file.AFileSpec
//import swaydb.core.segment.ASegmentSpec
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//
//import scala.collection.mutable.ListBuffer
//import swaydb.testkit.TestKit._
//
//class SegmentBlockSpec extends ASegmentSpec with AFileSpec {
//
//  val keyValueCount = 100
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  implicit def testTimer: TestTimer = TestTimer.random
//  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random
//  implicit val ec = TestExecutionContext.executionContext
//
//  "SegmentBlock" should {
//    "convert empty KeyValues and not throw exception but return empty bytes" in {
//      val sortedIndexConfig = SortedIndexBlockConfig.random
//
//      val closedSegment =
//        SegmentBlock.writeOneOrMany(
//          mergeStats =
//            MergeStats
//              .persistentBuilder[Memory](ListBuffer.empty)
//              .close(
//                hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//                optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//              ),
//          createdInLevel = randomIntMax(Int.MaxValue),
//          valuesConfig = ValuesBlockConfig.random,
//          sortedIndexConfig = sortedIndexConfig,
//          binarySearchIndexConfig = BinarySearchIndexBlockConfig.random,
//          hashIndexConfig = HashIndexBlockConfig.random,
//          bloomFilterConfig = BloomFilterBlockConfig.random,
//          segmentConfig = SegmentBlockConfig.random
//        ).awaitInf
//
//      closedSegment shouldBe empty
//    }
//
//    "converting KeyValues to bytes and execute readAll and find on the bytes" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          def test(keyValues: Slice[Memory]) = {
//            val segmentBytes =
//              SegmentBlock
//                .writeClosedOne(keyValues = keyValues)
//                .flattenSegmentBytes
//
//            val reader = Reader(segmentBytes)
//            assertReads(keyValues, reader)
//
//            val persistentReader = createRandomFileReader(segmentBytes)
//            assertReads(keyValues, persistentReader)
//            persistentReader.file.close()
//          }
//
//          runThis(100.times, log = true) {
//            val count = eitherOne(randomIntMax(20) max 1, 50, 100)
//            val keyValues = randomizedKeyValues(count, startId = Some(1))
//            if (keyValues.nonEmpty) test(keyValues)
//          }
//      }
//    }
//
//    "converting large KeyValues to bytes" in {
//      runThis(10.times, log = true) {
//        TestCaseSweeper {
//          implicit sweeper =>
//            import sweeper._
//            //increase the size of value to test it on larger values.
//            val keyValues = randomPutKeyValues(count = 100, valueSize = 10000, startId = Some(0))
//
//            val segmentBytes =
//              SegmentBlock
//                .writeClosedOne(keyValues = keyValues)
//                .flattenSegmentBytes
//
//            //in memory
//            assertReads(keyValues.toSlice, Reader(segmentBytes.cut()))
//            //on disk
//            assertReads(keyValues.toSlice, createRandomFileReader(segmentBytes))
//        }
//      }
//    }
//
//    "write and read Int min max key values" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          val keyValues = Slice(Memory.put(Int.MaxValue, Int.MinValue), Memory.put(Int.MinValue, Int.MaxValue))
//
//          val (bytes, deadline) =
//            SegmentBlock
//              .writeClosedOne(keyValues = keyValues)
//              .flattenSegment
//
//          deadline shouldBe empty
//
//          //in memory
//          assertReads(keyValues, Reader(bytes))
//          //on disk
//          assertReads(keyValues, createRandomFileReader(bytes))
//      }
//    }
//
//    "write and read Keys with None value to a Slice[Byte]" in {
//      runThis(10.times) {
//        TestCaseSweeper {
//          implicit sweeper =>
//            import sweeper._
//            val setDeadlines = randomBoolean()
//
//            val keyValues =
//              randomFixedNoneValue(
//                count = randomIntMax(1000) max 1,
//                startId = Some(1),
//                addPutDeadlines = setDeadlines,
//                addUpdateDeadlines = setDeadlines,
//                addRemoveDeadlines = setDeadlines
//              )
//
//            keyValues foreach {
//              keyValue =>
//                keyValue.value.toOptionC shouldBe empty
//            }
//
//            val (bytes, deadline) =
//              SegmentBlock
//                .writeClosedOne(keyValues)
//                .flattenSegment
//
//            if (!setDeadlines) deadline shouldBe empty
//
//            //in memory
//            assertReads(keyValues, Reader(bytes))
//            //on disk
//            assertReads(keyValues, createRandomFileReader(bytes))
//        }
//      }
//    }
//    //
//    //    "report Segment corruption if CRC check does not match when reading the footer" in {
//    //      //FIXME - flaky tests
//    //
//    //      //      runThis(100.times) {
//    //      //        val keyValues = Slice(Memory.put(1))
//    //      //
//    //      //        val (bytes, _) =
//    //      //          SegmentBlock.write(
//    //      //            keyValues = keyValues,
//    //      //            segmentConfig =
//    //      //      SegmentBlockConfig(
//    //      //        blockIO = dataType => BlockIO.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
//    //      //        compressions = Seq.empty
//    //      //      ),
//    //      //            createdInLevel = 0
//    //      //          ).runIO.flattenSegment
//    //      //
//    //      //        //        val result = SegmentBlock.read(SegmentBlockOffset(0, bytes.size), Reader(bytes.drop(2)))
//    //      //        //        if(result.isSuccess)
//    //      //        //          println("debug")
//    //      //        //        result.failed.runIO.exception shouldBe a[SegmentCorruptionException]
//    //      //
//    //      //        SegmentBlock.read(SegmentBlockOffset(0, bytes.size), Reader(bytes)) map {
//    //      //          segmentBlock =>
//    //      //            SegmentBlock.readFooter(segmentBlock.createBlockReader(Reader(bytes.drop(1)))).failed.runIO.exception shouldBe a[SegmentCorruptionException]
//    //      //            SegmentBlock.readFooter(segmentBlock.createBlockReader(Reader(bytes.dropRight(1)))).failed.runIO.exception shouldBe a[SegmentCorruptionException]
//    //      //            SegmentBlock.readFooter(segmentBlock.createBlockReader(Reader(bytes.slice(10, 20)))).failed.runIO.exception shouldBe a[SegmentCorruptionException]
//    //      //        } get
//    //      //      }
//    //    }
//  }
//
//  "SegmentFooter.read" should {
//    "set hasRange to false when Segment contains no Range key-value" in {
//      runThis(100.times) {
//        TestCaseSweeper {
//          implicit sweeper =>
//            import sweeper._
//            val keyValues = randomizedKeyValues(keyValueCount, addRanges = false)
//            if (keyValues.nonEmpty) {
//
//              val blocks = getBlocksSingle(keyValues).get
//
//              blocks.footer.keyValueCount shouldBe keyValues.size
//              blocks.footer.hasRange shouldBe false
//            }
//        }
//      }
//    }
//
//    "set hasRange to true and mightContainRemoveRange to false when Segment does not contain Remove range or function or pendingApply with function or remove but has other ranges" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          def doAssert(keyValues: Slice[Memory]) = {
//            val expectedHasRemoveRange = keyValues.exists(_.mightContainRemoveRange)
//
//            MergeStats.persistentBuilder(keyValues).mightContainRemoveRange shouldBe expectedHasRemoveRange
//
//            val blocks = getBlocksSingle(keyValues).get
//
//            if (expectedHasRemoveRange) blocks.bloomFilterReader shouldBe empty
//
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.hasRange shouldBe true
//          }
//
//          runThis(100.times) {
//            doAssert(randomizedKeyValues(keyValueCount, addRangeRemoves = false, addRanges = true, startId = Some(1)))
//          }
//      }
//    }
//
//    "set hasRange & mightContainRemoveRange to true and not create bloomFilter when Segment contains Remove range key-value" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          def doAssert(keyValues: Slice[Memory]) = {
//            keyValues.exists(_.mightContainRemoveRange) shouldBe true
//
//            val blocks =
//              getBlocksSingle(
//                keyValues = keyValues,
//                bloomFilterConfig =
//                  BloomFilterBlockConfig(
//                    falsePositiveRate = 0.001,
//                    minimumNumberOfKeys = 1,
//                    optimalMaxProbe = probe => probe,
//                    ioStrategy = _ => randomIOStrategy(),
//                    compressions = _ => randomCompressions()
//                  )
//              ).get
//
//            blocks.bloomFilterReader shouldBe empty
//
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.hasRange shouldBe true
//            //bloom filters do
//            blocks.footer.bloomFilterOffset shouldBe empty
//          }
//
//          runThis(100.times) {
//            val keyValues =
//              randomizedKeyValues(keyValueCount, startId = Some(1)) ++
//                Slice(
//                  Memory.Range(
//                    fromKey = 20,
//                    toKey = 21,
//                    fromValue = randomFromValueOption(),
//                    rangeValue = Value.remove(randomDeadlineOption)
//                  )
//                )
//
//            doAssert(keyValues)
//          }
//      }
//    }
//
//    "set hasRange to false when there are no ranges" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          def doAssert(keyValues: Slice[Memory]) = {
//            keyValues.exists(_.mightContainRemoveRange) shouldBe false
//
//            val blocks =
//              getBlocksSingle(
//                keyValues = keyValues,
//                bloomFilterConfig =
//                  BloomFilterBlockConfig.random.copy(
//                    falsePositiveRate = 0.0001,
//                    minimumNumberOfKeys = 0
//                  )
//              ).get
//
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.rangeCount shouldBe keyValues.count(_.isRange)
//            blocks.bloomFilterReader shouldBe defined
//            blocks.footer.bloomFilterOffset shouldBe defined
//            blocks.bloomFilterReader shouldBe defined
//            assertBloom(keyValues, blocks.bloomFilterReader.get)
//          }
//
//          runThis(100.times) {
//            val keyValues = randomizedKeyValues(keyValueCount, addRanges = false, addRangeRemoves = false)
//            if (keyValues.nonEmpty) doAssert(keyValues)
//          }
//      }
//    }
//
//    "set mightContainRemoveRange to true, hasGroup to true & not create bloomFilter when only the group contains remove range" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          def doAssert(keyValues: Slice[Memory]) = {
//            keyValues.exists(_.mightContainRemoveRange) shouldBe true
//
//            val blocks =
//              getBlocksSingle(
//                keyValues = keyValues,
//                bloomFilterConfig =
//                  BloomFilterBlockConfig.random.copy(
//                    falsePositiveRate = 0.0001,
//                    minimumNumberOfKeys = 0
//                  )
//              ).get
//
//            blocks.footer.keyValueCount shouldBe keyValues.size
//            blocks.footer.hasRange shouldBe true
//            blocks.footer.bloomFilterOffset shouldBe empty
//            blocks.bloomFilterReader shouldBe empty
//          }
//
//          runThis(100.times) {
//            doAssert(
//              Slice(
//                randomFixedKeyValue(1),
//                randomFixedKeyValue(2),
//                randomPutKeyValue(10, "val"),
//                randomRangeKeyValue(12, 15, rangeValue = Value.remove(None))
//              )
//            )
//          }
//      }
//    }
//  }
//
//  "writing key-values with duplicate values" should {
//    "use the same valueOffset and not create duplicate values" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          import sweeper._
//          runThis(1000.times) {
//            //make sure the first byte in the value is not the same as the key (just for the this test).
//            val fixedValue: Slice[Byte] = Slice(11.toByte) ++ randomBytesSlice(randomIntMax(50)).drop(1)
//
//            def fixed: Slice[Memory.Fixed] =
//              Slice(
//                Memory.put(1, fixedValue),
//                Memory.update(2, fixedValue),
//                Memory.put(3, fixedValue),
//                Memory.put(4, fixedValue),
//                Memory.update(5, fixedValue),
//                Memory.put(6, fixedValue),
//                Memory.update(7, fixedValue),
//                Memory.update(8, fixedValue),
//                Memory.put(9, fixedValue),
//                Memory.update(10, fixedValue)
//              )
//
//            val applies = randomApplies(deadline = None)
//
//            def pendingApply: Slice[Memory.PendingApply] =
//              Slice(
//                Memory.PendingApply(1, applies),
//                Memory.PendingApply(2, applies),
//                Memory.PendingApply(3, applies),
//                Memory.PendingApply(4, applies),
//                Memory.PendingApply(5, applies),
//                Memory.PendingApply(6, applies),
//                Memory.PendingApply(7, applies)
//              )
//
//            val keyValues =
//              eitherOne(
//                left = fixed,
//                right = pendingApply
//              )
//
//            //value the first value for either fixed or range.
//            //this value is only expected to be written ones.
//            keyValues.head.value.toOptionC shouldBe defined
//            val value = keyValues.head.value.getC
//
//            val blocks =
//              getBlocksSingle(
//                keyValues,
//                valuesConfig =
//                  ValuesBlockConfig(
//                    compressDuplicateValues = true,
//                    compressDuplicateRangeValues = randomBoolean(),
//                    ioStrategy = _ => randomIOAccess(),
//                    compressions = _ => Seq.empty
//                  )
//              ).get
//
//            val bytes = blocks.valuesReader.value.readAllAndGetReader().readRemaining()
//
//            //only the bytes of the first value should be set and the next byte should be the start of index
//            //as values are not duplicated
//            bytes.take(value.size) shouldBe value
//            //drop the first value bytes that are value bytes and the next value bytes (value of the next key-value) should not be value bytes.
//            bytes.drop(value.size).take(value.size) should not be value
//
//            val readKeyValues = SortedIndexBlock.toSlice(blocks.footer.keyValueCount, blocks.sortedIndexReader, blocks.valuesReader.orNull)
//            readKeyValues should have size keyValues.size
//
//            //assert that all valueOffsets of all key-values are the same
//            readKeyValues.foldLeft(Option.empty[Int]) {
//              case (previousOffsetOption, fixed: Persistent.Fixed) =>
//                previousOffsetOption match {
//                  case Some(previousOffset) =>
//                    fixed.valueOffset shouldBe previousOffset
//                    fixed.valueLength shouldBe value.size
//                    previousOffsetOption
//
//                  case None =>
//                    Some(fixed.valueOffset)
//                }
//
//              case keyValue =>
//                fail(s"Got: ${keyValue.getClass.getSimpleName}. Didn't expect any other key-value other than Put")
//            }
//          }
//      }
//    }
//  }
//}
