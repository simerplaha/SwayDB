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
package swaydb.core.segment.format.a.block.binarysearch

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.SortedIndexBlock
import swaydb.core.{Blocks, TestBase, TestSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Try

class BinarySearchIndexBlock_Segment_RandomSearch_Spec extends TestBase with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  implicit val blockCacheMemorySweeper = TestSweeper.memorySweeperBlock

  val startId = 0

  def genKeyValuesAndBlocks(keyValuesCount: Int = randomIntMax(500) max 1): (Slice[Transient], Blocks) = {
    //  def genKeyValuesAndBlocks(keyValuesCount: Int = 50): (Slice[Transient], Blocks) = {

    val keyValues =
      randomizedKeyValues(
        count = keyValuesCount,
        startId = Some(startId)
        //        addGroups = false,
        //        addRanges = false
      ).updateStats(
        sortedIndexConfig =
          SortedIndexBlock.Config.random.copy(
            //            normaliseIndex = true,
            prefixCompressionResetCount = (randomIntMax(keyValuesCount) - 1) max 0,
            //            enablePartialRead = true,
            //            enableAccessPositionIndex = true
          ),
        binarySearchIndexConfig =
          BinarySearchIndexBlock.Config.random.copy(
            enabled = true,
            minimumNumberOfKeys = 0,
            //            fullIndex = keyValuesCount < 10
            fullIndex = true
          )
      )

    keyValues foreach {
      keyValue =>
      //println(s"Key: ${keyValue.key.readInt()}. isPrefixCompressed: ${keyValue.isPrefixCompressed}: ${keyValue.getClass.getSimpleName}")
    }

    val blocks = getBlocks(keyValues).value

    blocks.binarySearchIndexReader foreach {
      binarySearchIndexReader =>
        println
        println(s"binarySearchIndexReader.valuesCount: ${binarySearchIndexReader.block.valuesCount}")
        println(s"binarySearchIndexReader.isFullIndex: ${binarySearchIndexReader.block.isFullIndex}")
    }

    println(s"sortedIndexReader.enableAccessPositionIndex: ${blocks.sortedIndexReader.block.enableAccessPositionIndex}")
    println(s"sortedIndexReader.isNormalisedBinarySearchable: ${blocks.sortedIndexReader.block.isNormalisedBinarySearchable}")
    println(s"sortedIndexReader.hasPrefixCompression: ${blocks.sortedIndexReader.block.hasPrefixCompression}")

    (blocks.binarySearchIndexReader.isDefined || blocks.sortedIndexReader.block.isNormalisedBinarySearchable) shouldBe true

    (keyValues, blocks)
  }

  "fully indexed search" should {

    "search key-values" in {

      runThis(10.times, log = true, s"Running binary search test") {
        val (keyValues, blocks) = genKeyValuesAndBlocks()

        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent.Partial]) {
          case (previous, (keyValue, index)) =>

            //println
            //println(s"Key: ${keyValue.key.readInt()}")
            val start: Option[Persistent.Partial] =
              eitherOne(None, previous)

            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            //println("--- For end ---")
            val end: Option[Persistent.Partial] =
            eitherOne(
              left = None,
              right = //There is a random test. It could get index out of bounds.
                Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                  keyValue =>
                    //read the end key from index.
                    BinarySearchIndexBlock.search(
                      key = keyValue.key,
                      lowest = eitherOne(None, start),
                      highest = None,
                      keyValuesCount = keyValues.size,
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).toOption
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
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ) match {
                case BinarySearchGetResult.None(_) =>
                  //all keys are known to exist.
                  fail("Expected success")

                case BinarySearchGetResult.Some(value) =>
                  value.key shouldBe keyValue.key
                  Some(value)
              }

            //            found.value shouldBe keyValue
            eitherOne(None, found, previous)
          //            found
        }
      }
    }

    "search non existent key-values" in {
      runThis(10.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()
        val higherStartFrom = keyValues.last.key.readInt() + 1000000
        (higherStartFrom to higherStartFrom + 100) foreach {
          key =>
            //println
            //println(s"find: $key")
            BinarySearchIndexBlock.search(
              key = key,
              lowest = None,
              highest = None,
              keyValuesCount = blocks.footer.keyValueCount,
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ) match {
              case BinarySearchGetResult.None(lower) =>
                //lower will always be the last known uncompressed key before the last key-value.
                if (keyValues.size > 4)
                  keyValues.dropRight(1).reverse.find(!_.isPrefixCompressed) foreach {
                    expectedLower =>
                      lower.value.key shouldBe expectedLower.key
                  }

              case _: BinarySearchGetResult.Some[_] =>
                fail("Didn't expect a match")
            }
        }

        (0 until startId) foreach {
          key =>
            //println
            //println(s"find: $key")
            BinarySearchIndexBlock.search(
              key = key,
              lowest = None,
              highest = None,
              keyValuesCount = blocks.footer.keyValueCount,
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ) match {
              case BinarySearchGetResult.None(lower) =>
                //lower is always empty since the test keys are lower than the actual key-values.
                lower shouldBe empty

              case _: BinarySearchGetResult.Some[_] =>
                fail("Didn't expect a math")
            }
        }
      }
    }

    "search higher for existing key-values" in {
      runThis(10.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()
        //test higher in reverse order
        keyValues.zipWithIndex.foldRight(Option.empty[Persistent.Partial]) {
          case ((keyValue, index), expectedHigher) =>

            //println(s"\nKey: ${keyValue.key.readInt()}")

            //println("--- Start ---")
            val start: Option[Persistent.Partial] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(randomIntMax(index))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        lowest = None,
                        highest = None,
                        keyValuesCount = blocks.footer.keyValueCount,
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).toOption.map(_.toPersistent)
                  }
              )
            //println("--- Start ---")

            //println("--- End ---")
            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            val end: Option[Persistent.Partial] =
            eitherOne(
              left = None,
              right = //There is a random test. It could get index out of bounds.
                Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                  keyValue =>
                    //read the end key from index.
                    BinarySearchIndexBlock.search(
                      key = keyValue.key,
                      lowest = eitherOne(None, start),
                      highest = None,
                      keyValuesCount = blocks.footer.keyValueCount,
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).toOption
                }
            )
            //println("--- End ---")

            def getHigher(key: Slice[Byte]) =
              BinarySearchIndexBlock.searchHigher(
                key = key,
                start = start,
                end = end,
                keyValuesCount = blocks.footer.keyValueCount,
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              )

            keyValue match {
              case fixed: Transient.Fixed =>
                val higher = getHigher(fixed.key)
                //println(s"Higher: ${higher.map(_.key.readInt())}")
                if (index == keyValues.size - 1)
                  higher shouldBe empty
                else
                  higher.value.key shouldBe expectedHigher.value.key

              case range: Transient.Range =>
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
              startFrom = eitherOne(start, None),
              fullRead = true,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            )
            //println("--- End next higher ---")
            nextHigher
        }
      }
    }

    "search lower for existing key-values" in {
      runThis(10.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()

        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
          case (expectedLower, (keyValue, index)) =>

            //println
            //println(s"Key: ${keyValue.key.readInt()}. Expected lower: ${expectedLower.map(_.key.readInt())}")

            //println("--- Start ---")
            val start: Option[Persistent.Partial] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(randomIntMax(index))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        lowest = None,
                        highest = None,
                        keyValuesCount = blocks.footer.keyValueCount,
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).toOption
                  }
              )
            //println("--- Start ---")
            //              None

            //println("--- END ---")
            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            val end: Option[Persistent.Partial] =
            eitherOne(
              left = None,
              right = //There is a random test. It could get index out of bounds.
                //                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                Try(keyValues(index)).toOption flatMap {
                  //                  Try(keyValues(index + randomIntMax(index))).toOption flatMap {
                  endKeyValue =>
                    //read the end key from index.
                    //                      if (endKeyValue.isRange && endKeyValue.key == keyValue.key)
                    //                        None
                    //                      else
                    BinarySearchIndexBlock.search(
                      key = endKeyValue.key,
                      lowest = orNone(start),
                      highest = None,
                      keyValuesCount = blocks.footer.keyValueCount,
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).toOption
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
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              )

            //          //println(s"Lower for: ${keyValue.minKey.readInt()}")

            //println
            //println("--- SEARCHING ---")
            keyValue match {
              case fixed: Transient.Fixed =>
                val lower = getLower(fixed.key)
                if (index == 0)
                  lower shouldBe empty
                else
                  lower.value.key shouldBe expectedLower.value.key

              case range: Transient.Range =>
                val lower = getLower(range.fromKey)
                if (index == 0)
                  lower shouldBe empty
                else
                  lower.map(_.toPersistent) shouldBe expectedLower

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
                    lower.value shouldBe range
                }
            }
            //println("--- SEARCHING ---")

            //get the persistent key-value for the next lower assert.
            //println
            //println(" --- lower for next ---")
            val got =
            SortedIndexBlock.seekAndMatch(
              key = keyValue.key,
              startFrom = None,
              fullRead = true,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).map(_.toPersistent)
            //println(" --- lower for next ---")

            got.value.key shouldBe keyValue.key
            got
        }
      }
    }
  }
}
