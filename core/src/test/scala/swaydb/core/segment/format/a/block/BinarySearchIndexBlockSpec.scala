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

import org.scalamock.scalatest.MockFactory
import swaydb.Error.Segment.ExceptionHandler
import swaydb.{Error, IO}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.{Blocks, TestBase}
import swaydb.core.TestData._
import swaydb.core.cache.Cache
import swaydb.core.data.{Persistent, Time, Transient}
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.IOValues._
import org.scalatest.OptionValues._

import scala.util.Try

class BinarySearchIndexBlockSpec extends TestBase with MockFactory {

  implicit val keyOrder = KeyOrder.default

  "write full index" when {
    def assertSearch(bytes: Slice[Byte],
                     byteSizePerValue: Int,
                     values: Seq[Int]) =
      runThis(10.times) {
        val largestValue = values.last

        def matcher(valueToFind: Int, valueFound: Int): IO[swaydb.Error.Segment, KeyMatcher.Result] =
          IO {
            //            //println(s"valueToFind: $valueToFind. valueFound: $valueFound")
            if (valueToFind == valueFound)
              KeyMatcher.Result.Matched(None, null, None)
            else if (valueToFind < valueFound)
              KeyMatcher.Result.AheadOrNoneOrEnd
            else
              KeyMatcher.Result.BehindFetchNext(null)
          }

        def context(valueToFind: Int) =
          new BinarySearchContext {
            val bytesPerValue: Int = byteSizePerValue
            val valuesCount: Int = values.size
            val isFullIndex: Boolean = true
            val higherOrLower: Option[Boolean] = None
            val lowestKeyValue: Option[Persistent] = None
            val highestKeyValue: Option[Persistent] = None

            def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
              val foundValue =
                if (bytesPerValue == 4)
                  bytes.take(offset, bytesPerValue).readInt()
                else
                  bytes.take(offset, bytesPerValue).readIntUnsigned().value

              matcher(valueToFind, foundValue)
            }
          }

        values foreach {
          value =>
            BinarySearchIndexBlock.binarySearch(context(value)).value shouldBe a[BinaryGet.Some[_]]
        }

        //check for items not in the index.
        val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)

        notInIndex foreach {
          i =>
            BinarySearchIndexBlock.binarySearch(context(i)).value shouldBe a[BinaryGet.None[_]]
        }
      }

    "all values have the same size" in {
      runThis(10.times) {
        Seq(0 to 127, 128 to 300, 16384 to 16384 + 200, Int.MaxValue - 5000 to Int.MaxValue - 1000) foreach {
          values =>
            val valuesCount = values.size
            val largestValue = values.last
            val state =
              BinarySearchIndexBlock.State(
                largestValue = largestValue,
                uniqueValuesCount = valuesCount,
                isFullIndex = true,
                minimumNumberOfKeys = 0,
                compressions = _ => Seq.empty
              ).value

            values foreach {
              offset =>
                BinarySearchIndexBlock.write(value = offset, state = state).value
            }

            BinarySearchIndexBlock.close(state).value

            state.writtenValues shouldBe values.size

            state.bytes.isFull shouldBe true

            Seq(
              BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
              BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
            ) foreach {
              reader =>
                val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value

                val decompressedBytes = Block.unblock(reader.copy()).value.readFullBlock().value

                block.valuesCount shouldBe state.writtenValues

                //byte size of Int.MaxValue is 5, but the index will switch to using 4 byte ints.
                block.bytesPerValue should be <= 4

                assertSearch(decompressedBytes, block.bytesPerValue, values)
            }
        }
      }
    }

    "all values have unique size" in {
      runThis(10.times) {
        //generate values of uniques sizes.
        val values = (126 to 130) ++ (16384 - 2 to 16384)
        val valuesCount = values.size
        val largestValue = values.last
        val compression = eitherOne(Seq.empty, Seq(randomCompression()))

        val state =
          BinarySearchIndexBlock.State(
            largestValue = largestValue,
            uniqueValuesCount = valuesCount,
            isFullIndex = true,
            minimumNumberOfKeys = 0,
            compressions = _ => compression
          ).value

        values foreach {
          value =>
            BinarySearchIndexBlock.write(value = value, state = state).value
        }
        BinarySearchIndexBlock.close(state).value

        state.writtenValues shouldBe values.size

        Seq(
          BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
          BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
        ) foreach {
          reader =>
            val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value

            val decompressedBytes = Block.unblock(reader.copy()).value.readFullBlock().value

            block.bytesPerValue shouldBe Bytes.sizeOf(largestValue)

            block.valuesCount shouldBe values.size

            assertSearch(decompressedBytes, block.bytesPerValue, values)
        }
      }
    }
  }

  "search" when {
    val values = Slice(1, 5, 10)
    val valuesCount = values.size
    val largestValue = values.last
    val compression = eitherOne(Seq.empty, Seq(randomCompression()))

    val state =
      BinarySearchIndexBlock.State(
        largestValue = largestValue,
        uniqueValuesCount = valuesCount,
        isFullIndex = true,
        minimumNumberOfKeys = 0,
        compressions = _ => compression
      ).value

    values foreach {
      value =>
        BinarySearchIndexBlock.write(value = value, state = state).value
    }

    BinarySearchIndexBlock.close(state).value

    state.writtenValues shouldBe 3

    "1" in {
      //1, 5, 10
      Seq(
        BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).value,
        BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
      ) foreach {
        reader =>
          val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).value).value

          block.bytesPerValue shouldBe Bytes.sizeOf(largestValue)
          block.valuesCount shouldBe values.size

          val unblocked = Block.unblock[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](reader.copy()).value
          val unblockedBytes = unblocked.readFullBlock().value

          unblocked.size.value shouldBe 3

          //1, 5, 10
          //1
          val one =
          Persistent.Put.fromCache(
            key = 1,
            deadline = None,
            valueCache = Cache.emptyValuesBlock,
            time = Time.empty,
            nextIndexOffset = randomIntMax(),
            nextIndexSize = randomIntMax(),
            indexOffset = randomIntMax(),
            valueOffset = 0,
            valueLength = 0,
            sortedIndexAccessPosition = eitherOne(0, 1)
          )

          val five =
            Persistent.Put.fromCache(
              key = 5,
              deadline = None,
              valueCache = Cache.emptyValuesBlock,
              time = Time.empty,
              nextIndexOffset = randomIntMax(),
              nextIndexSize = randomIntMax(),
              indexOffset = randomIntMax(),
              valueOffset = 0,
              valueLength = 0,
              sortedIndexAccessPosition = eitherOne(0, 1)
            )

          val matchResultForOne = KeyMatcher.Result.Matched(None, one, None)
          KeyMatcher.Get(1).apply(one, None, randomBoolean()) shouldBe matchResultForOne

          val matchResultForFive = KeyMatcher.Result.AheadOrNoneOrEnd
          KeyMatcher.Get(1).apply(five, None, randomBoolean()) shouldBe matchResultForFive

          val matcher = mockFunction[Int, IO[swaydb.Error.Segment, KeyMatcher.Result]]("matcher")
          matcher.expects(5) returns IO.Right(matchResultForFive)
          matcher.expects(1) returns IO.Right(matchResultForOne)

          val context =
            new BinarySearchContext {
              val bytesPerValue: Int = block.bytesPerValue
              val valuesCount: Int = values.size
              val isFullIndex: Boolean = true
              val higherOrLower: Option[Boolean] = None
              val lowestKeyValue: Option[Persistent] = None
              val highestKeyValue: Option[Persistent] = None

              def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
                val value = unblockedBytes.take(offset, bytesPerValue).readIntUnsigned().value
                matcher(value)
              }
            }

          //search get
          BinarySearchIndexBlock.binarySearch(context).value match {
            case BinaryGet.None(_) =>
              fail()

            case BinaryGet.Some(lower, value) =>
              lower shouldBe empty
              value shouldBe one
          }
      }
    }
  }

  "fully indexed search" should {
    val startId = 0

    def genKeyValuesAndBlocks(keyValuesCount: Int = randomIntMax(1000) max 1): (Slice[Transient], Blocks) = {

      val keyValues =
        randomizedKeyValues(
          count = keyValuesCount,
          startId = Some(startId),
          //          addGroups = false,
          //          addRanges = true
        ).updateStats(
          sortedIndexConfig =
            SortedIndexBlock.Config.random.copy(
              //              normaliseIndex = true,
              //              prefixCompressionResetCount = 3,
              //              enablePartialRead = true,
              //              enableAccessPositionIndex = true
            ),
          binarySearchIndexConfig =
            BinarySearchIndexBlock.Config.random.copy(
              enabled = true,
              minimumNumberOfKeys = 0,
              fullIndex = keyValuesCount < 10
            )
        )

      keyValues foreach {
        keyValue =>
        //println(s"Key: ${keyValue.key.readInt()}. isPrefixCompressed: ${keyValue.isPrefixCompressed}: ${keyValue.getClass.getSimpleName}")
      }

      val blocks = getBlocks(keyValues).value

      //      blocks.binarySearchIndexReader foreach {
      //      binarySearchIndexReader =>
      //println
      //println(s"binarySearchIndexReader.valuesCount: ${binarySearchIndexReader.block.valuesCount}")
      //println(s"binarySearchIndexReader.isFullIndex: ${binarySearchIndexReader.block.isFullIndex}")
      //      }

      //println(s"sortedIndexReader.enableAccessPositionIndex: ${blocks.sortedIndexReader.block.enableAccessPositionIndex}")
      //println(s"sortedIndexReader.isNormalisedBinarySearchable: ${blocks.sortedIndexReader.block.isNormalisedBinarySearchable}")
      //println(s"sortedIndexReader.hasPrefixCompression: ${blocks.sortedIndexReader.block.hasPrefixCompression}")

      (blocks.binarySearchIndexReader.isDefined || blocks.sortedIndexReader.block.isNormalisedBinarySearchable) shouldBe true

      (keyValues, blocks)
    }

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
                      keyValuesCount = IO(keyValues.size),
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).value.toOption
                }
            )
            //println("--- End end ---")
            //println
            //              None

            ////println(s"Find: ${keyValue.minKey.readInt()}")

            val found =
              BinarySearchIndexBlock.search(
                key = keyValue.key,
                lowest = start,
                highest = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).value match {
                case BinaryGet.None(_) =>
                  //all keys are known to exist.
                  fail("Expected success")

                case BinaryGet.Some(lower, value) =>
                  if (value.isInstanceOf[Persistent.Partial.GroupT])
                  //println("debug")
                  //if startFrom is given, search either return a more nearest lowest of the passed in lowest.
                  //                  if (index > 0) {
                  //                    if (lower.isEmpty)
                  //                      //println("debug")
                  //                    lower shouldBe defined
                  //                  }
                  //                  if (start.isDefined) lower shouldBe defined

                    lower foreach {
                      lower =>
                        ////println(s"Lower: ${lower.key.readInt()}, startFrom: ${start.map(_.key.readInt())}")
                        //lower should always be less than keyValue's key.
                        lower.key.readInt() should be < keyValue.key.readInt()
                        start foreach {
                          from =>
                            //lower should be greater than the supplied lower or should be equals.
                            //seek should not result in another lower key-value which is smaller than the input start key-value.
                            lower.key.readInt() should be >= from.key.readInt()
                        }
                    }
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
              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).value match {
              case BinaryGet.None(lower) =>
                //lower will always be the last known uncompressed key before the last key-value.
                if (keyValues.size > 4)
                  keyValues.dropRight(1).reverse.find(!_.isPrefixCompressed) foreach {
                    expectedLower =>
                      lower.value.key shouldBe expectedLower.key
                  }

              case _: BinaryGet.Some[_] =>
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
              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).value match {
              case BinaryGet.None(lower) =>
                //lower is always empty since the test keys are lower than the actual key-values.
                lower shouldBe empty

              case _: BinaryGet.Some[_] =>
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
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).value.toOption.map(_.toPersistent.get)
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
                      keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).value.toOption
                }
            )
            //println("--- End ---")

            def getHigher(key: Slice[Byte]) =
              BinarySearchIndexBlock.searchHigher(
                key = key,
                start = start,
                end = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).value

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

              case group: Transient.Group =>
                (group.key.readInt() until group.maxKey.maxKey.readInt()) foreach {
                  key =>
                    val actualHigher = getHigher(key).value
                    actualHigher.key shouldBe group.key
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
            ).value
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
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).value.toOption
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
                  endKeyValue =>
                    //read the end key from index.
                    //                      if (endKeyValue.isRange && endKeyValue.key == keyValue.key)
                    //                        None
                    //                      else
                    BinarySearchIndexBlock.search(
                      key = endKeyValue.key,
                      lowest = orNone(start),
                      highest = None,
                      keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                      binarySearchIndexReader = blocks.binarySearchIndexReader,
                      sortedIndexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).value.toOption
                }
            )
            //println("--- END ---")
            //              None

            def getLower(key: Slice[Byte]) =
              BinarySearchIndexBlock.searchLower(
                key = key,
                start = start,
                end = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).value

            //          //println(s"Lower for: ${keyValue.minKey.readInt()}")

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
                  lower.map(_.toPersistent.value) shouldBe expectedLower

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

              case group: Transient.Group =>
                //do lower on Group's minKey first
                val lower = getLower(group.key)
                if (index == 0)
                  lower shouldBe empty
                else
                  lower.value shouldBe expectedLower.value

                (group.key.readInt() + 1 to group.maxKey.maxKey.readInt()) foreach {
                  key =>
                    //println
                    //println(s"Key: $key")
                    val lower = getLower(key)
                    lower.value.key shouldBe group.key
                }
            }

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
            ).value.map(_.toPersistent.value)
            //println(" --- lower for next ---")

            got.value.key shouldBe keyValue.key
            got
        }
      }
    }
  }
}
