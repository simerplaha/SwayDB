/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package swaydb.core.segment.block.binarysearch

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.data.{Memory, Persistent, PersistentOption, SegmentKeyOrders}
import swaydb.core.{ACoreSpec, SegmentBlocks, TestSweeper}
import swaydb.core.segment.ASegmentSpec
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._

import scala.util.Try
import swaydb.testkit.TestKit._

class BinarySearchIndexBlock_Segment_RandomSearch_Spec extends ASegmentSpec with MockFactory {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = SegmentKeyOrders(keyOrder).partialKeyOrder

  val startId = 0

  def genKeyValuesAndBlocks(keyValuesCount: Int = 10)(implicit testCaseSweeper: TestSweeper): (Slice[Memory], SegmentBlocks) = {
    import testCaseSweeper._
    //  def genKeyValuesAndBlocks(keyValuesCount: Int = 50): (Slice[Memory], Blocks) = {

    val keyValues =
      randomizedKeyValues(
        count = keyValuesCount,
        startId = Some(startId)
        //        addRanges = false,
        //        addFunctions = false,
        //        addRemoves = false,
        //        addUpdates = false,
        //        addPendingApply = false
      )

    //    keyValues foreach {
    //      keyValue =>
    //println(s"Key: ${keyValue.key.readInt()}. isPrefixCompressed: ${keyValue.isPrefixCompressed}: ${keyValue.getClass.getSimpleName}")
    //    }

    val blocks =
      getBlocks(
        keyValues = keyValues,
        useCacheableReaders = randomBoolean(),
        sortedIndexConfig =
          SortedIndexBlockConfig.random.copy(
            //            normaliseIndex = true,
            shouldPrefixCompress = eitherOne(_ => false, _ => true, _ => randomBoolean())
            //            enablePartialRead = true,
            //            enableAccessPositionIndex = true
          ),
        binarySearchIndexConfig =
          BinarySearchIndexBlockConfig.random.copy(
            enabled = true,
            minimumNumberOfKeys = 0,
            //            fullIndex = keyValuesCount < 10
            fullIndex = true
          ),
        segmentConfig =
          SegmentBlockConfig.random2(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue)
      ).value

    blocks should have size 1

    val segmentBlock = blocks.head

    segmentBlock.binarySearchIndexReader foreach {
      binarySearchIndexReader =>
        println
        println(s"binarySearchIndexReader.valuesCount: ${binarySearchIndexReader.block.valuesCount}")
        println(s"binarySearchIndexReader.isFullIndex: ${binarySearchIndexReader.block.isFullIndex}")
    }

    println(s"sortedIndexReader.enableAccessPositionIndex: ${segmentBlock.sortedIndexReader.block.enableAccessPositionIndex}")
    println(s"sortedIndexReader.isNormalisedBinarySearchable: ${segmentBlock.sortedIndexReader.block.isBinarySearchable}")
    println(s"sortedIndexReader.hasPrefixCompression: ${segmentBlock.sortedIndexReader.block.hasPrefixCompression}")

    (segmentBlock.binarySearchIndexReader.isDefined || segmentBlock.sortedIndexReader.block.isBinarySearchable) shouldBe true

    (keyValues, segmentBlock)
  }

  "fully indexed search" should {

    "search key-values" in {

      runThis(100.times, log = true, s"Running binary search test") {
        TestSweeper {
          implicit sweeper =>
            val (keyValues, blocks) = genKeyValuesAndBlocks()

            keyValues.zipWithIndex.foldLeft(Persistent.Null: PersistentOption) {
              case (previous, (keyValue, index)) =>

                //println
                //println(s"Key: ${keyValue.key.readInt()}")
                val start: PersistentOption =
                  eitherOne(Persistent.Null, previous)

                //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
                //println("--- For end ---")
                val end: PersistentOption =
                eitherOne(
                  left = Persistent.Null,
                  right = //There is a random test. It could get index out of bounds.
                    Try(keyValues(index + randomIntMax(keyValues.size - 1)))
                      .toOption
                      .flatMapOption(Persistent.Null: PersistentOption) {
                        keyValue =>
                          //read the end key from index.
                          BinarySearchIndexBlock.search(
                            key = keyValue.key,
                            lowest = eitherOne(Persistent.Null, start),
                            highest = Persistent.Null,
                            keyValuesCount = keyValues.size,
                            binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                            sortedIndexReader = blocks.sortedIndexReader,
                            valuesReaderOrNull = blocks.valuesReader.orNull
                          ).toPersistentOptional
                      }
                )
                //println("--- End end ---")
                //println
                //              None

                //            //println(s"Find: ${keyValue.minKey.readInt()}")

                val found =
                  BinarySearchIndexBlock.search(
                    key = keyValue.key,
                    lowest = start,
                    highest = end,
                    keyValuesCount = blocks.footer.keyValueCount,
                    binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                    sortedIndexReader = blocks.sortedIndexReader,
                    valuesReaderOrNull = blocks.valuesReader.orNull
                  ) match {
                    case Persistent.Partial.Null =>
                      //all keys are known to exist.
                      fail("Expected success")

                    case some: Persistent.Partial =>
                      some.key shouldBe keyValue.key
                      some.toPersistent
                  }

                //            found.value shouldBe keyValue
                eitherOne(Persistent.Null, found, previous)
              //            found
            }
        }
      }
    }

    "search non existent key-values" in {
      runThis(100.times, log = true) {
        TestSweeper {
          implicit sweeper =>

            val (keyValues, blocks) = genKeyValuesAndBlocks()
            val higherStartFrom = keyValues.last.key.readInt() + 1000000
            (higherStartFrom to higherStartFrom + 100) foreach {
              key =>
                //println
                //println(s"find: $key")
                BinarySearchIndexBlock.search(
                  key = key,
                  lowest = Persistent.Null,
                  highest = Persistent.Null,
                  keyValuesCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ) match {
                  case Persistent.Partial.Null =>
                  //lower will always be the last known uncompressed key before the last key-value.
                  //                if (keyValues.size > 4)
                  //                  keyValues.dropRight(1).reverse.find(!_.isPrefixCompressed) foreach {
                  //                    expectedLower =>
                  //                      none.lower.value.key shouldBe expectedLower.key
                  //                  }

                  case some: Persistent.Partial =>
                    if (some.isBinarySearchMatched)
                      fail("Didn't expect a match")
                }
            }

            (0 until startId) foreach {
              key =>
                //println
                //println(s"find: $key")
                BinarySearchIndexBlock.search(
                  key = key,
                  lowest = Persistent.Null,
                  highest = Persistent.Null,
                  keyValuesCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ) match {
                  case Persistent.Partial.Null =>
                  //lower is always empty since the test keys are lower than the actual key-values.

                  case some: Persistent.Partial =>
                    if (some.isBinarySearchMatched)
                      fail("Didn't expect a match")
                }
            }
        }
      }
    }

    "search higher for existing key-values" in {
      runThis(100.times, log = true) {
        TestSweeper {
          implicit sweeper =>

            val (keyValues, blocks) = genKeyValuesAndBlocks()
            //test higher in reverse order
            keyValues.zipWithIndex.foldRight(Persistent.Null: PersistentOption) {
              case ((keyValue, index), expectedHigher) =>

                //println(s"\nKey: ${keyValue.key.readInt()}")

                //println("--- Start ---")
                val start: PersistentOption =
                  eitherOne(
                    left = Persistent.Null,
                    right = //There is a random test. It could get index out of bounds.
                      Try(keyValues(randomIntMax(index)))
                        .toOption
                        .flatMapOption(Persistent.Null: PersistentOption) {
                          keyValue =>
                            //read the end key from index.
                            BinarySearchIndexBlock.search(
                              key = keyValue.key,
                              lowest = Persistent.Null,
                              highest = Persistent.Null,
                              keyValuesCount = blocks.footer.keyValueCount,
                              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                              sortedIndexReader = blocks.sortedIndexReader,
                              valuesReaderOrNull = blocks.valuesReader.orNull
                            ).toPersistentOptional
                        }
                  )
                //println("--- Start ---")

                //println("--- End ---")
                //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
                val end: PersistentOption =
                eitherOne(
                  left = Persistent.Null,
                  right = //There is a random test. It could get index out of bounds.
                    Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption
                      .flatMapOption(Persistent.Null: PersistentOption) {
                        keyValue =>
                          //read the end key from index.
                          BinarySearchIndexBlock.search(
                            key = keyValue.key,
                            lowest = eitherOne(Persistent.Null, start),
                            highest = Persistent.Null,
                            keyValuesCount = blocks.footer.keyValueCount,
                            binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                            sortedIndexReader = blocks.sortedIndexReader,
                            valuesReaderOrNull = blocks.valuesReader.orNull
                          ).toPersistentOptional
                      }
                )
                //println("--- End ---")

                def getHigher(key: Slice[Byte]) =
                  BinarySearchIndexBlock.searchHigher(
                    key = key,
                    start = start,
                    end = end,
                    keyValuesCount = blocks.footer.keyValueCount,
                    binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                    sortedIndexReader = blocks.sortedIndexReader,
                    valuesReaderOrNull = blocks.valuesReader.orNull
                  ).toOption

                keyValue match {
                  case fixed: Memory.Fixed =>
                    val higher = getHigher(fixed.key)
                    //println(s"Higher: ${higher.map(_.key.readInt())}")
                    if (index == keyValues.size - 1)
                      higher shouldBe empty
                    else
                      higher.value.key shouldBe expectedHigher.getS.key

                  case range: Memory.Range =>
                    (range.fromKey.readInt() until range.toKey.readInt()) foreach {
                      key =>
                        val higher = getHigher(key)
                        //println(s"Higher: ${higher.map(_.key.readInt())}")
                        //println(s"Key: $key")
                        higher.value shouldBe range
                    }
                }

                //get the persistent key-value for the next higher assert.
                //println
                //println("--- Next higher ---")
                val nextHigher =
                SortedIndexBlock.seekAndMatch(
                  key = keyValue.key,
                  startFrom = eitherOne(start, Persistent.Null),
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                )
                //println("--- End next higher ---")
                nextHigher
            }
        }
      }
    }

    "search lower for existing key-values" in {
      runThis(100.times, log = true) {
        TestSweeper {
          implicit sweeper =>

            val (keyValues, blocks) = genKeyValuesAndBlocks()

            keyValues.zipWithIndex.foldLeft(Persistent.Null: PersistentOption) {
              case (expectedLower, (keyValue, index)) =>

                //println
                //println(s"Key: ${keyValue.key.readInt()}. Expected lower: ${expectedLower.map(_.key.readInt())}")

                //println("--- Start ---")
                val start: PersistentOption =
                  eitherOne(
                    left = Persistent.Null,
                    right = //There is a random test. It could get index out of bounds.
                      Try(keyValues(randomIntMax(index))).toOption
                        .flatMapOption(Persistent.Null: PersistentOption) {
                          keyValue =>
                            //read the end key from index.
                            BinarySearchIndexBlock.search(
                              key = keyValue.key,
                              lowest = Persistent.Null,
                              highest = Persistent.Null,
                              keyValuesCount = blocks.footer.keyValueCount,
                              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                              sortedIndexReader = blocks.sortedIndexReader,
                              valuesReaderOrNull = blocks.valuesReader.orNull
                            ).toPersistentOptional
                        }
                  )
                //println("--- Start ---")
                //              None

                //println("--- END ---")
                //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.

                val end: PersistentOption =
                  eitherOne(
                    left = Persistent.Null,
                    right = //There is a random test. It could get index out of bounds.
                    //                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                      Try(keyValues(index))
                        .toOption
                        .flatMapOption(Persistent.Null: PersistentOption) {
                          //                  Try(keyValues(index + randomIntMax(index))).toOption flatMap {
                          endKeyValue =>
                            //read the end key from index.
                            //                      if (endKeyValue.isRange && endKeyValue.key == keyValue.key)
                            //                        None
                            //                      else
                            BinarySearchIndexBlock.search(
                              key = endKeyValue.key,
                              lowest = eitherOne(start, Persistent.Null),
                              highest = Persistent.Null,
                              keyValuesCount = blocks.footer.keyValueCount,
                              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                              sortedIndexReader = blocks.sortedIndexReader,
                              valuesReaderOrNull = blocks.valuesReader.orNull
                            ).toPersistentOptional
                        }
                  )
                //println("--- END ---")
                //              None

                def getLower(key: Slice[Byte]) =
                  BinarySearchIndexBlock.searchLower(
                    key = key,
                    start = start,
                    end = end,
                    keyValuesCount = blocks.footer.keyValueCount,
                    binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                    sortedIndexReader = blocks.sortedIndexReader,
                    valuesReaderOrNull = blocks.valuesReader.orNull
                  )

                //          //println(s"Lower for: ${keyValue.minKey.readInt()}")

                //println
                //println("--- SEARCHING ---")
                keyValue match {
                  case fixed: Memory.Fixed =>
                    val lower = getLower(fixed.key)
                    if (index == 0)
                      lower.toOptionS shouldBe empty
                    else
                      lower.getS.key shouldBe expectedLower.getS.key

                  case range: Memory.Range =>
                    val lower = getLower(range.fromKey)
                    if (index == 0)
                      lower.toOptionS shouldBe empty
                    else
                      lower shouldBe expectedLower

                    val from = range.fromKey.readInt() + 1
                    val to = range.toKey.readInt()

                    //println
                    //println(s"Range: $from -> $to")
                    //do lower on within range keys
                    (from to to) foreach {
                      key =>
                        //println
                        //println(s"Key: $key")
                        val lower = getLower(key)
                        lower.getS shouldBe range
                    }
                }
                //println("--- SEARCHING ---")

                //get the persistent key-value for the next lower assert.
                //println
                //println(" --- lower for next ---")
                val got =
                SortedIndexBlock.seekAndMatch(
                  key = keyValue.key,
                  startFrom = Persistent.Null,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                )
                //println(" --- lower for next ---")

                got.getS.key shouldBe keyValue.key
                got
            }
        }
      }
    }
  }
}
