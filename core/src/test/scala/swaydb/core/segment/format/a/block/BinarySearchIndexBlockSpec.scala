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
//package swaydb.core.segment.format.a.block
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.{Error, IO}
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.{Blocks, TestBase}
//import swaydb.core.TestData._
//import swaydb.core.cache.Cache
//import swaydb.core.data.{Persistent, Time, Transient}
//import swaydb.core.segment.format.a.block.reader.BlockRefReader
//import swaydb.core.util.Bytes
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.IOValues._
//import org.scalatest.OptionValues._
//
//import scala.util.Try
//
//class BinarySearchIndexBlockSpec extends TestBase with MockFactory {
//
//  implicit val keyOrder = KeyOrder.default
//
//  "write full index" when {
//    def assertSearch(bytes: Slice[Byte],
//                     byteSizePerValue: Int,
//                     values: Seq[Int]) =
//      runThis(10.times) {
//        val largestValue = values.last
//
//        def matcher(valueToFind: Int, valueFound: Int): IO[swaydb.Error.Segment, KeyMatcher.Result] =
//          IO {
//            //            println(s"valueToFind: $valueToFind. valueFound: $valueFound")
//            if (valueToFind == valueFound)
//              KeyMatcher.Result.Matched(None, null, None)
//            else if (valueToFind < valueFound)
//              KeyMatcher.Result.AheadOrNoneOrEnd
//            else
//              KeyMatcher.Result.BehindFetchNext(null)
//          }
//
//        def context(valueToFind: Int) =
//          new BinarySearchContext {
//            val bytesPerValue: Int = byteSizePerValue
//            val valuesCount: Int = values.size
//            val isFullIndex: Boolean = true
//            val higherOrLower: Option[Boolean] = None
//            val lowestKeyValue: Option[Persistent] = None
//            val highestKeyValue: Option[Persistent] = None
//
//            def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
//              val foundValue =
//                if (bytesPerValue == 4)
//                  bytes.take(offset, bytesPerValue).readInt()
//                else
//                  bytes.take(offset, bytesPerValue).readIntUnsigned().value
//
//              matcher(valueToFind, foundValue)
//            }
//          }
//
//        values foreach {
//          value =>
//            BinarySearchIndexBlock.search(startIndex = None, endIndex = None, context(value)).value shouldBe a[SearchResult.Some[_]]
//        }
//
//        //check for items not in the index.
//        val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)
//
//        notInIndex foreach {
//          i =>
//            BinarySearchIndexBlock.search(startIndex = None, endIndex = None, context(i)).value shouldBe a[SearchResult.None[_]]
//        }
//      }
//
//    "all values have the same size" in {
//      runThis(10.times) {
//        Seq(0 to 127, 128 to 300, 16384 to 16384 + 200, Int.MaxValue - 5000 to Int.MaxValue - 1000) foreach {
//          values =>
//            val valuesCount = values.size
//            val largestValue = values.last
//            val state =
//              BinarySearchIndexBlock.State(
//                largestValue = largestValue,
//                uniqueValuesCount = valuesCount,
//                isFullIndex = true,
//                minimumNumberOfKeys = 0,
//                compressions = _ => Seq.empty
//              ).value
//
//            values foreach {
//              offset =>
//                BinarySearchIndexBlock.write(value = offset, state = state).value
//            }
//
//            BinarySearchIndexBlock.close(state).value
//
//            state.writtenValues shouldBe values.size
//
//            state.bytes.isFull shouldBe true
//
//            Seq(
//              BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
//              BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
//            ) foreach {
//              reader =>
//                val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value
//
//                val decompressedBytes = Block.unblock(reader.copy()).value.readFullBlock().value
//
//                block.valuesCount shouldBe state.writtenValues
//
//                //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
//                block.bytesPerValue should be <= 4
//
//                assertSearch(decompressedBytes, block.bytesPerValue, values)
//            }
//        }
//      }
//    }
//
//    "all values have unique size" in {
//      runThis(10.times) {
//        //generate values of uniques sizes.
//        val values = (126 to 130) ++ (16384 - 2 to 16384)
//        val valuesCount = values.size
//        val largestValue = values.last
//        val compression = eitherOne(Seq.empty, Seq(randomCompression()))
//
//        val state =
//          BinarySearchIndexBlock.State(
//            largestValue = largestValue,
//            uniqueValuesCount = valuesCount,
//            isFullIndex = true,
//            minimumNumberOfKeys = 0,
//            compressions = _ => compression
//          ).value
//
//        values foreach {
//          value =>
//            BinarySearchIndexBlock.write(value = value, state = state).value
//        }
//        BinarySearchIndexBlock.close(state).value
//
//        state.writtenValues shouldBe values.size
//
//        Seq(
//          BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
//          BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
//        ) foreach {
//          reader =>
//            val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value
//
//            val decompressedBytes = Block.unblock(reader.copy()).value.readFullBlock().value
//
//            block.bytesPerValue shouldBe Bytes.sizeOf(largestValue)
//
//            block.valuesCount shouldBe values.size
//
//            assertSearch(decompressedBytes, block.bytesPerValue, values)
//        }
//      }
//    }
//  }
//
//  "search" when {
//    val values = Slice(1, 5, 10)
//    val valuesCount = values.size
//    val largestValue = values.last
//    val compression = eitherOne(Seq.empty, Seq(randomCompression()))
//
//    val state =
//      BinarySearchIndexBlock.State(
//        largestValue = largestValue,
//        uniqueValuesCount = valuesCount,
//        isFullIndex = true,
//        minimumNumberOfKeys = 0,
//        compressions = _ => compression
//      ).value
//
//    values foreach {
//      value =>
//        BinarySearchIndexBlock.write(value = value, state = state).value
//    }
//
//    BinarySearchIndexBlock.close(state).value
//
//    state.writtenValues shouldBe 3
//
//    "1" in {
//      //1, 5, 10
//      Seq(
//        BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
//        BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
//      ) foreach {
//        reader =>
//          val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value
//
//          block.bytesPerValue shouldBe Bytes.sizeOf(largestValue)
//          block.valuesCount shouldBe values.size
//
//          val unblocked = Block.unblock[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](reader.copy()).value
//          val unblockedBytes = unblocked.readFullBlock().value
//
//          unblocked.size.value shouldBe 3
//
//          //1, 5, 10
//          //1
//          val one =
//          Persistent.Put.fromCache(
//            key = 1,
//            deadline = None,
//            valueCache = Cache.emptyValuesBlock,
//            time = Time.empty,
//            nextIndexOffset = randomIntMax(),
//            nextIndexSize = randomIntMax(),
//            indexOffset = randomIntMax(),
//            valueOffset = 0,
//            valueLength = 0,
//            sortedIndexAccessPosition = -1,
//            sortedIndexAccessPosition = binarySearchIndexPosition = eitherOne(0, 1),
//            isPrefixCompressed = randomBoolean()
//          )
//
//          val five =
//            Persistent.Put.fromCache(
//              key = 5,
//              deadline = None,
//              valueCache = Cache.emptyValuesBlock,
//              time = Time.empty,
//              nextIndexOffset = randomIntMax(),
//              nextIndexSize = randomIntMax(),
//              indexOffset = randomIntMax(),
//              valueOffset = 0,
//              valueLength = 0,
//              sortedIndexAccessPosition = eitherOne(0, 1),
//              isPrefixCompressed = randomBoolean()
//            )
//
//          val matchResultForOne = KeyMatcher.Result.Matched(None, one, None)
//          KeyMatcher.Get(1).apply(one, None, randomBoolean()) shouldBe matchResultForOne
//
//          val matchResultForFive = KeyMatcher.Result.AheadOrNoneOrEnd
//          KeyMatcher.Get(1).apply(five, None, randomBoolean()) shouldBe matchResultForFive
//
//          val matcher = mockFunction[Int, IO[swaydb.Error.Segment, KeyMatcher.Result]]("matcher")
//          matcher.expects(5) returns IO.Right(matchResultForFive)
//          matcher.expects(1) returns IO.Right(matchResultForOne)
//
//          val context =
//            new BinarySearchContext {
//              val bytesPerValue: Int = block.bytesPerValue
//              val valuesCount: Int = values.size
//              val isFullIndex: Boolean = true
//              val higherOrLower: Option[Boolean] = None
//              val lowestKeyValue: Option[Persistent] = None
//              val highestKeyValue: Option[Persistent] = None
//
//              def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
//                val value = unblockedBytes.take(offset, bytesPerValue).readIntUnsigned().value
//                matcher(value)
//              }
//            }
//
//          //search get
//          BinarySearchIndexBlock.search(startIndex = None, endIndex = None, context).value match {
//            case SearchResult.None(_) =>
//              fail()
//
//            case SearchResult.Some(lower, value, _) =>
//              lower shouldBe empty
//              value shouldBe one
//          }
//      }
//    }
//  }
//
//  "fully indexed search" should {
//    val startId = 0
//
//    def genKeyValuesAndBlocks(keyValuesCount: Int = 100): (Slice[Transient], Blocks) = {
//
//      val keyValues =
//        randomizedKeyValues(
//          count = keyValuesCount,
//          startId = Some(startId),
//          addGroups = false,
//          addRanges = false
//        ).updateStats(
//          sortedIndexConfig =
//            SortedIndexBlock.Config.random.copy(
//              normaliseIndex = false,
//              prefixCompressionResetCount = 0
//            ),
//          binarySearchIndexConfig =
//            BinarySearchIndexBlock.Config.random.copy(
//              enabled = true,
//              minimumNumberOfKeys = 0,
//              fullIndex = true,
//              searchSortedIndexDirectlyIfPossible = false
//            )
//        )
//
//      val blocks = getBlocks(keyValues).value
//
//      blocks.binarySearchIndexReader match {
//        case Some(binarySearchIndexReader) =>
//          binarySearchIndexReader.block.isFullIndex shouldBe true
//
//        case None =>
//          blocks.sortedIndexReader.block.isNormalisedBinarySearchable shouldBe true
//          blocks.sortedIndexReader.block.hasPrefixCompression shouldBe false
//      }
//
//      (keyValues, blocks)
//    }
//
//    "search key-values" in {
//      runThis(1.times, log = true, s"Running binary search test") {
//        val (keyValues, blocks) = genKeyValuesAndBlocks()
//
//        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent.Partial]) {
//          case (previous, (keyValue, index)) =>
//
//            println(s"Key: ${keyValue.key.readInt()}")
//            val start: Option[Persistent.Partial] =
//            //              eitherOne(None, previous)
//              None
//
//            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
//            val end: Option[Persistent.Partial] =
//            //              eitherOne(
//            //                left = None,
//            //                right = //There is a random test. It could get index out of bounds.
//            //                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
//            //                    keyValue =>
//            //                      //read the end key from index.
//            //                      BinarySearchIndexBlock.search(
//            //                        key = keyValue.key,
//            //                        start = eitherOne(None, start),
//            //                        end = None,
//            //                        keyValuesCount = IO(keyValues.size),
//            //                        binarySearchIndexReader = blocks.binarySearchIndexReader,
//            //                        sortedIndexReader = blocks.sortedIndexReader,
//            //                        valuesReader = blocks.valuesReader
//            //                      ).value.toOption
//            //                  }
//            //              )
//              None
//
//            //println(s"Find: ${keyValue.minKey.readInt()}")
//
//            val found =
//              BinarySearchIndexBlock.search(
//                key = keyValue.key,
//                lowest = start,
//                highest = end,
//                startIndex = None,
//                endIndex = None,
//                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//                binarySearchIndexReader = blocks.binarySearchIndexReader,
//                sortedIndexReader = blocks.sortedIndexReader,
//                valuesReader = blocks.valuesReader
//              ).value match {
//                case SearchResult.None(_) =>
//                  //all keys are known to exist.
//                  fail("Expected success")
//
//                case SearchResult.Some(lower, value, _) =>
//                  //if startFrom is given, search either return a more nearest lowest of the passed in lowest.
//                  //                  if (index > 0) {
//                  //                    if (lower.isEmpty)
//                  //                      println("debug")
//                  //                    lower shouldBe defined
//                  //                  }
//                  //                  if (start.isDefined) lower shouldBe defined
//
//                  lower foreach {
//                    lower =>
//                      //println(s"Lower: ${lower.key.readInt()}, startFrom: ${start.map(_.key.readInt())}")
//                      //lower should always be less than keyValue's key.
//                      lower.key.readInt() should be < keyValue.key.readInt()
//                      start foreach {
//                        from =>
//                          //lower should be greater than the supplied lower or should be equals.
//                          //seek should not result in another lower key-value which is smaller than the input start key-value.
//                          lower.key.readInt() should be >= from.key.readInt()
//                      }
//                  }
//                  value.key shouldBe keyValue.key
//                  Some(value)
//              }
//
//            found.value shouldBe keyValue
//            eitherOne(None, found, previous)
//        }
//      }
//    }
//
//    "search non existent key-values" in {
//      runThis(100.times, log = true) {
//        val (keyValues, blocks) = genKeyValuesAndBlocks()
//        val higherStartFrom = keyValues.last.key.readInt() + 1000000
//        (higherStartFrom to higherStartFrom + 100) foreach {
//          key =>
//            //          println(s"find: $key")
//            BinarySearchIndexBlock.search(
//              key = key,
//              lowest = None,
//              highest = None,
//              startIndex = None,
//              endIndex = None,
//              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//              binarySearchIndexReader = blocks.binarySearchIndexReader,
//              sortedIndexReader = blocks.sortedIndexReader,
//              valuesReader = blocks.valuesReader
//            ).value match {
//              case SearchResult.None(lower) =>
//                //lower will always be the last known uncompressed key before the last key-value.
//                if (keyValues.size > 4)
//                  keyValues.dropRight(1).reverse.find(!_.isPrefixCompressed) foreach {
//                    expectedLower =>
//                      lower.value.key shouldBe expectedLower.key
//                  }
//
//              case _: SearchResult.Some[_] =>
//                fail("Didn't expect a match")
//            }
//        }
//
//        (0 until startId) foreach {
//          key =>
//            //          println(s"find: $key")
//            BinarySearchIndexBlock.search(
//              key = key,
//              lowest = None,
//              highest = None,
//              startIndex = None,
//              endIndex = None,
//              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//              binarySearchIndexReader = blocks.binarySearchIndexReader,
//              sortedIndexReader = blocks.sortedIndexReader,
//              valuesReader = blocks.valuesReader
//            ).value match {
//              case SearchResult.None(lower) =>
//                //lower is always empty since the test keys are lower than the actual key-values.
//                lower shouldBe empty
//
//              case _: SearchResult.Some[_] =>
//                fail("Didn't expect a math")
//            }
//        }
//      }
//    }
//
//    "search higher for existing key-values" in {
//      runThis(1.times, log = true) {
//        val (keyValues, blocks) = genKeyValuesAndBlocks()
//        //test higher in reverse order
//        keyValues.zipWithIndex.foldRight(Option.empty[Persistent.Partial]) {
//          case ((keyValue, index), expectedHigher) =>
//
//            println(s"Key: ${keyValue.key.readInt()}")
//
//            val start: Option[Persistent.Partial] =
//            //              eitherOne(
//            //                left = None,
//            //                right = //There is a random test. It could get index out of bounds.
//            //                  Try(keyValues(randomIntMax(index))).toOption flatMap {
//            //                    keyValue =>
//            //                      //read the end key from index.
//            //                      BinarySearchIndexBlock.search(
//            //                        key = keyValue.key,
//            //                        start = None,
//            //                        end = None,
//            //                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//            //                        binarySearchIndexReader = blocks.binarySearchIndexReader,
//            //                        sortedIndexReader = blocks.sortedIndexReader,
//            //                        valuesReader = blocks.valuesReader
//            //                      ).value.toOption.map(_.toPersistent.get)
//            //                  }
//            //              )
//              None
//
//            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
//            val end: Option[Persistent.Partial] =
//            //              eitherOne(
//            //                left = None,
//            //                right = //There is a random test. It could get index out of bounds.
//            //                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
//            //                    keyValue =>
//            //                      //read the end key from index.
//            //                      BinarySearchIndexBlock.search(
//            //                        key = keyValue.key,
//            //                        start = eitherOne(None, start),
//            //                        end = None,
//            //                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//            //                        binarySearchIndexReader = blocks.binarySearchIndexReader,
//            //                        sortedIndexReader = blocks.sortedIndexReader,
//            //                        valuesReader = blocks.valuesReader
//            //                      ).value.toOption
//            //                  }
//            //              )
//              None
//
//            def getHigher(key: Slice[Byte]) =
//              BinarySearchIndexBlock.searchHigher(
//                key = key,
//                start = start,
//                end = end,
//                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//                binarySearchIndexReader = blocks.binarySearchIndexReader,
//                sortedIndexReader = blocks.sortedIndexReader,
//                valuesReader = blocks.valuesReader
//              ).value
//
//            keyValue match {
//              case fixed: Transient.Fixed =>
//                val higher = getHigher(fixed.key)
//                println(s"Higher: ${higher.toOption.map(_.key.readInt())}")
//                higher match {
//                  case SearchResult.None(lower) =>
//                    lower.value.key.readInt() should be <= fixed.key.readInt()
//                    expectedHigher shouldBe empty
//
//                  case SearchResult.Some(lower, actualHigher, _) =>
//                    //                    lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
//                    lower.value.key.readInt() should be <= fixed.key.readInt()
//                    actualHigher.key shouldBe expectedHigher.value.key
//                }
//
//              case range: Transient.Range =>
//                (range.fromKey.readInt() until range.toKey.readInt()) foreach {
//                  key =>
//                    val higher = getHigher(key)
//                    println(s"Higher: ${higher.toOption.map(_.key.readInt())}")
//                    higher match {
//                      case SearchResult.None(lower) =>
//                        lower.value.key.readInt() should be <= key
//                        expectedHigher shouldBe empty
//
//                      case SearchResult.Some(lower, actualHigher, _) =>
//                        println(s"Key: $key")
//                        println("Lower: " + lower.map(keyValue => keyValue.key.readInt()))
//
//                        //                        if (index > 0 && key > range.fromKey.readInt()) {
//                        //                          if(lower.isEmpty)
//                        //                            println("debug")
//                        //                          lower.value.key.readInt() should be <= key
//                        //                        }
//                        actualHigher shouldBe range
//                    }
//                }
//              //
//              case group: Transient.Group =>
//                (group.key.readInt() until group.maxKey.maxKey.readInt()) foreach {
//                  key =>
//                    getHigher(key) match {
//                      case SearchResult.None(lower) =>
//                        lower.value.key.readInt() should be <= key
//                        expectedHigher shouldBe empty
//
//                      case SearchResult.Some(lower, actualHigher, _) =>
//                        lower.value.key.readInt() should be <= key
//                        actualHigher.key shouldBe group.key
//                    }
//                }
//            }
//
//            //get the persistent key-value for the next higher assert.
//            SortedIndexBlock.search(
//              key = keyValue.key,
//              startFrom = eitherOne(start, None),
//              fullRead = true,
//              sortedIndexReader = blocks.sortedIndexReader,
//              valuesReader = blocks.valuesReader
//            ).value
//        }
//      }
//    }
//
//    "search lower for existing key-values" in {
//      runThis(1.times, log = true) {
//        val (keyValues, blocks) = genKeyValuesAndBlocks()
//
//        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
//          case (expectedLower, (keyValue, index)) =>
//
//            println(s"Key: ${keyValue.key.readInt()}. Expected lower: ${expectedLower.map(_.key.readInt())}")
//
//            val start: Option[Persistent.Partial] =
////              eitherOne(
////                left = None,
////                right = //There is a random test. It could get index out of bounds.
////                  Try(keyValues(randomIntMax(index))).toOption flatMap {
////                    keyValue =>
////                      //read the end key from index.
////                      BinarySearchIndexBlock.search(
////                        key = keyValue.key,
////                        start = orNone(expectedLower),
////                        end = None,
////                        overwriteStart = None,
////                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
////                        binarySearchIndexReader = blocks.binarySearchIndexReader,
////                        sortedIndexReader = blocks.sortedIndexReader,
////                        valuesReader = blocks.valuesReader
////                      ).value.toOption
////                  }
////              )
//            None
//
//            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
//            val end: Option[Persistent.Partial] =
////              eitherOne(
////                left = None,
////                right = //There is a random test. It could get index out of bounds.
////                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
////                    keyValue =>
////                      //read the end key from index.
////                      BinarySearchIndexBlock.search(
////                        key = keyValue.key,
////                        start = orNone(start),
////                        end = None,
////                        overwriteStart = None,
////                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
////                        binarySearchIndexReader = blocks.binarySearchIndexReader,
////                        sortedIndexReader = blocks.sortedIndexReader,
////                        valuesReader = blocks.valuesReader
////                      ).value.toOption
////                  }
////              )
//            None
//
//            def getLower(key: Slice[Byte]) =
//              BinarySearchIndexBlock.searchLower(
//                key = key,
//                start = start,
//                end = end,
//                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
//                binarySearchIndexReader = blocks.binarySearchIndexReader,
//                sortedIndexReader = blocks.sortedIndexReader,
//                valuesReader = blocks.valuesReader
//              ).value
//
//            //          println(s"Lower for: ${keyValue.minKey.readInt()}")
//
//            keyValue match {
//              case fixed: Transient.Fixed =>
//                getLower(fixed.key) match {
//                  case SearchResult.None(lower) =>
//                    if (index == 0)
//                      lower shouldBe empty
//                    else
//                      fail("Didn't expect None")
//
//                  case SearchResult.Some(lower, actualLower, _) =>
////                    lower shouldBe empty
//                    actualLower.key shouldBe expectedLower.value.key
//                }
//
//              case range: Transient.Range =>
//                //do a lower on fromKey first.
//                getLower(range.fromKey) match {
//                  case SearchResult.None(lower) =>
//                    if (index == 0)
//                      lower shouldBe empty
//                    else
//                      fail("Didn't expect None")
//
//                  case SearchResult.Some(lower, actualLower, _) =>
//                    lower shouldBe empty
//                    actualLower shouldBe expectedLower.value
//                }
//
//                //do lower on within range keys
//                (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
//                  key =>
//                    getLower(key) match {
//                      case SearchResult.None(lower) =>
//                        fail("Didn't expect None")
//
//                      case SearchResult.Some(lower, actualLower, _) =>
//                        lower shouldBe empty
//                        actualLower shouldBe range
//                    }
//                }
//
//              case group: Transient.Group =>
//                //do lower on Group's minKey first
//                getLower(group.key) match {
//                  case SearchResult.None(lower) =>
//                    if (index == 0)
//                      lower shouldBe empty
//                    else
//                      fail("Didn't expect None")
//
//                  case SearchResult.Some(lower, actualLower, _) =>
//                    lower shouldBe empty
//                    actualLower shouldBe expectedLower.value
//                }
//
//                (group.key.readInt() + 1 to group.maxKey.maxKey.readInt()) foreach {
//                  key =>
//                    getLower(key) match {
//                      case SearchResult.None(_) =>
//                        fail("Didn't expect None")
//
//                      case SearchResult.Some(lower, actualLower, _) =>
//                        lower shouldBe empty
//                        actualLower.key shouldBe group.key
//                    }
//                }
//            }
//
//            //get the persistent key-value for the next lower assert.
//            val got =
//              SortedIndexBlock.search(
//                key = keyValue.key,
//                startFrom = None,
//                fullRead = randomBoolean(),
//                sortedIndexReader = blocks.sortedIndexReader,
//                valuesReader = blocks.valuesReader
//              ).value.map(_.toPersistent.value)
//
//            got.value.key shouldBe keyValue.key
//            got
//        }
//      }
//    }
//  }
//}
