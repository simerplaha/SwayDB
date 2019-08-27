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
import swaydb.Error.Segment.ErrorHandler
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
            //            println(s"valueToFind: $valueToFind. valueFound: $valueFound")
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
            val startKeyValue: Option[Persistent] = None
            val endKeyValue: Option[Persistent] = None
            def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
              val foundValue =
                if (bytesPerValue == 4)
                  bytes.take(offset, bytesPerValue).readInt()
                else
                  bytes.take(offset, bytesPerValue).readIntUnsigned().get

              matcher(valueToFind, foundValue)
            }
          }

        values foreach {
          value =>
            BinarySearchIndexBlock.search(context(value)).get shouldBe a[SearchResult.Some[Int]]
        }

        //check for items not in the index.
        val notInIndex = (values.head - 100 until values.head) ++ (largestValue + 1 to largestValue + 100)

        notInIndex foreach {
          i =>
            BinarySearchIndexBlock.search(context(i)).get shouldBe a[SearchResult.None[Int]]
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
              ).get

            values foreach {
              offset =>
                BinarySearchIndexBlock.write(value = offset, state = state).get
            }

            BinarySearchIndexBlock.close(state).get

            state.writtenValues shouldBe values.size

            state.bytes.isFull shouldBe true

            Seq(
              BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).get,
              BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
            ) foreach {
              reader =>
                val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).get).get

                val decompressedBytes = Block.unblock(reader.copy()).get.readFullBlock().get

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
          ).get

        values foreach {
          value =>
            BinarySearchIndexBlock.write(value = value, state = state).get
        }
        BinarySearchIndexBlock.close(state).get

        state.writtenValues shouldBe values.size

        Seq(
          BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).get,
          BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
        ) foreach {
          reader =>
            val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).get).get

            val decompressedBytes = Block.unblock(reader.copy()).get.readFullBlock().get

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
      ).get

    values foreach {
      value =>
        BinarySearchIndexBlock.write(value = value, state = state).get
    }

    BinarySearchIndexBlock.close(state).get

    state.writtenValues shouldBe 3

    "1" in {
      //1, 5, 10
      Seq(
        BlockRefReader[BinarySearchIndexBlock.Offset](createRandomFileReader(state.bytes)).get,
        BlockRefReader[BinarySearchIndexBlock.Offset](state.bytes)
      ) foreach {
        reader =>
          val block = BinarySearchIndexBlock.read(Block.readHeader[BinarySearchIndexBlock.Offset](reader).get).get

          block.bytesPerValue shouldBe Bytes.sizeOf(largestValue)
          block.valuesCount shouldBe values.size

          val unblocked = Block.unblock[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](reader.copy()).get
          val unblockedBytes = unblocked.readFullBlock().get

          unblocked.size.get shouldBe 3

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
            accessPosition = eitherOne(0, 1),
            isPrefixCompressed = randomBoolean()
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
              accessPosition = eitherOne(0, 1),
              isPrefixCompressed = randomBoolean()
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
              val startKeyValue: Option[Persistent] = None
              val endKeyValue: Option[Persistent] = None
              def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] = {
                val value = unblockedBytes.take(offset, bytesPerValue).readIntUnsigned().get
                matcher(value)
              }
            }

          //search get
          BinarySearchIndexBlock.search(context).get match {
            case SearchResult.None(_) =>
              fail()

            case SearchResult.Some(lower, value) =>
              lower shouldBe empty
              value shouldBe one
          }
      }
    }
  }

  "fully indexed search" should {
    val startId = 100

    def genKeyValuesAndBlocks(keyValuesCount: Int = randomIntMax(1000) max 1): (Slice[Transient], Blocks) = {

      val keyValues =
        randomizedKeyValues(
          count = keyValuesCount,
          startId = Some(startId)
        ).updateStats(
          sortedIndexConfig =
            SortedIndexBlock.Config.random.copy(
              normaliseIndex = randomBoolean()
            ),
          binarySearchIndexConfig =
            BinarySearchIndexBlock.Config.random.copy(
              enabled = true,
              minimumNumberOfKeys = 0,
              fullIndex = true,
              searchSortedIndexDirectlyIfPossible = randomBoolean()
            )
        )

      val blocks = getBlocks(keyValues).get

      blocks.binarySearchIndexReader match {
        case Some(binarySearchIndexReader) =>
          binarySearchIndexReader.block.isFullIndex shouldBe true

        case None =>
          blocks.sortedIndexReader.block.isBinarySearchable shouldBe true
          blocks.sortedIndexReader.block.hasPrefixCompression shouldBe false
      }

      (keyValues, blocks)
    }

    "search key-values" in {
      runThis(100.times, log = true, s"Running binary search test") {
        val (keyValues, blocks) = genKeyValuesAndBlocks()

        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
          case (previous, (keyValue, index)) =>

            val start = eitherOne(None, previous)

            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            val end =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        start = eitherOne(None, start),
                        end = None,
                        keyValuesCount = IO(keyValues.size),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).get.toOption
                  }
              )

            //println(s"Find: ${keyValue.minKey.readInt()}")

            val found =
              BinarySearchIndexBlock.search(
                key = keyValue.key,
                start = start,
                end = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).get match {
                case SearchResult.None(lower) =>
                  //all keys are known to exist.
                  lower shouldBe empty
                  None

                case SearchResult.Some(lower, value) =>
                  //if startFrom is given, search either return a more nearest lowest of the passed in lowest.
                  if (start.isDefined) lower shouldBe defined

                  lower foreach {
                    lower =>
                      //println(s"Lower: ${lower.key.readInt()}, startFrom: ${start.map(_.key.readInt())}")
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

            found shouldBe keyValue
            eitherOne(None, found, previous)
        }
      }
    }

    "search non existent key-values" in {
      runThis(100.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()
        val higherStartFrom = keyValues.last.key.readInt() + 1000000
        (higherStartFrom to higherStartFrom + 100) foreach {
          key =>
            //          println(s"find: $key")
            BinarySearchIndexBlock.search(
              key = key,
              start = None,
              end = None,
              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get match {
              case SearchResult.None(lower) =>
                //lower will always be the last known uncompressed key before the last key-value.
                if (keyValues.size > 4)
                  keyValues.dropRight(1).reverse.find(!_.isPrefixCompressed) foreach {
                    expectedLower =>
                      lower.get.key shouldBe expectedLower.key
                  }

              case _: SearchResult.Some[_] =>
                fail("Didn't expect a match")
            }
        }

        (0 until startId) foreach {
          key =>
            //          println(s"find: $key")
            BinarySearchIndexBlock.search(
              key = key,
              start = None,
              end = None,
              keyValuesCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = blocks.binarySearchIndexReader,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get match {
              case SearchResult.None(lower) =>
                //lower is always empty since the test keys are lower than the actual key-values.
                lower shouldBe empty

              case _: SearchResult.Some[_] =>
                fail("Didn't expect a math")
            }
        }
      }
    }

    "search higher for existing key-values" in {
      runThis(100.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()
        //test higher in reverse order
        keyValues.zipWithIndex.foldRight(Option.empty[Persistent]) {
          case ((keyValue, index), expectedHigher) =>

            val start: Option[Persistent] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(randomIntMax(index))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        start = None,
                        end = None,
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).get.toOption
                  }
              )

            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            val end: Option[Persistent] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        start = eitherOne(None, start),
                        end = None,
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).get.toOption
                  }
              )

            def getHigher(key: Slice[Byte]) =
              BinarySearchIndexBlock.searchHigher(
                key = key,
                start = start,
                end = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).get

            keyValue match {
              case fixed: Transient.Fixed =>
                getHigher(fixed.key) match {
                  case SearchResult.None(lower) =>
                    if (keyValues.size > 4)
                      lower.get.key.readInt() should be < fixed.key.readInt()
                    expectedHigher shouldBe empty

                  case SearchResult.Some(lower, actualHigher) =>
                    lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                    actualHigher.key shouldBe expectedHigher.get.key
                }

              case range: Transient.Range =>
                (range.fromKey.readInt() until range.toKey.readInt()) foreach {
                  key =>
                    getHigher(key) match {
                      case SearchResult.None(lower) =>
                        lower.get.key.readInt() should be < key
                        expectedHigher shouldBe empty

                      case SearchResult.Some(lower, actualHigher) =>
                        lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                        actualHigher shouldBe range
                    }
                }
              //
              case group: Transient.Group =>
                (group.key.readInt() until group.maxKey.maxKey.readInt()) foreach {
                  key =>
                    getHigher(key) match {
                      case SearchResult.None(lower) =>
                        lower.get.key.readInt() should be < key
                        expectedHigher shouldBe empty

                      case SearchResult.Some(lower, actualHigher) =>
                        lower.foreach(_.key.readInt() should be < actualHigher.key.readInt())
                        actualHigher.key shouldBe group.key
                    }
                }
            }

            //get the persistent key-value for the next higher assert.
            SortedIndexBlock.search(
              key = keyValue.key,
              startFrom = eitherOne(start, None),
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get
        }
      }
    }

    "search lower for existing key-values" in {
      runThis(100.times, log = true) {
        val (keyValues, blocks) = genKeyValuesAndBlocks()

        keyValues.zipWithIndex.foldLeft(Option.empty[Persistent]) {
          case (expectedLower, (keyValue, index)) =>

            val start: Option[Persistent] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(randomIntMax(index))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        start = orNone(expectedLower),
                        end = None,
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).get.toOption
                  }
              )

            //randomly set start and end. Select a higher key-value which is a few indexes away from the actual key.
            val end: Option[Persistent] =
              eitherOne(
                left = None,
                right = //There is a random test. It could get index out of bounds.
                  Try(keyValues(index + randomIntMax(keyValues.size - 1))).toOption flatMap {
                    keyValue =>
                      //read the end key from index.
                      BinarySearchIndexBlock.search(
                        key = keyValue.key,
                        start = orNone(start),
                        end = None,
                        keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                        binarySearchIndexReader = blocks.binarySearchIndexReader,
                        sortedIndexReader = blocks.sortedIndexReader,
                        valuesReader = blocks.valuesReader
                      ).get.toOption
                  }
              )

            def getLower(key: Slice[Byte]) =
              BinarySearchIndexBlock.searchLower(
                key = key,
                start = start,
                end = end,
                keyValuesCount = IO.Right(blocks.footer.keyValueCount),
                binarySearchIndexReader = blocks.binarySearchIndexReader,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).get

            //          println(s"Lower for: ${keyValue.minKey.readInt()}")

            keyValue match {
              case fixed: Transient.Fixed =>
                getLower(fixed.key) match {
                  case SearchResult.None(lower) =>
                    if (index == 0)
                      lower shouldBe empty
                    else
                      fail("Didn't expect None")

                  case SearchResult.Some(lower, actualLower) =>
                    lower shouldBe empty
                    actualLower.key shouldBe expectedLower.get.key
                }

              case range: Transient.Range =>
                //do a lower on fromKey first.
                getLower(range.fromKey) match {
                  case SearchResult.None(lower) =>
                    if (index == 0)
                      lower shouldBe empty
                    else
                      fail("Didn't expect None")

                  case SearchResult.Some(lower, actualLower) =>
                    lower shouldBe empty
                    actualLower shouldBe expectedLower.get
                }

                //do lower on within range keys
                (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
                  key =>
                    getLower(key) match {
                      case SearchResult.None(lower) =>
                        fail("Didn't expect None")

                      case SearchResult.Some(lower, actualLower) =>
                        lower shouldBe empty
                        actualLower shouldBe range
                    }
                }

              case group: Transient.Group =>
                //do lower on Group's minKey first
                getLower(group.key) match {
                  case SearchResult.None(lower) =>
                    if (index == 0)
                      lower shouldBe empty
                    else
                      fail("Didn't expect None")

                  case SearchResult.Some(lower, actualLower) =>
                    lower shouldBe empty
                    actualLower shouldBe expectedLower.get
                }

                (group.key.readInt() + 1 to group.maxKey.maxKey.readInt()) foreach {
                  key =>
                    getLower(key) match {
                      case SearchResult.None(_) =>
                        fail("Didn't expect None")

                      case SearchResult.Some(lower, actualLower) =>
                        lower shouldBe empty
                        actualLower.key shouldBe group.key
                    }
                }
            }

            //get the persistent key-value for the next lower assert.
            val got =
              SortedIndexBlock.search(
                key = keyValue.key,
                startFrom = None,
                sortedIndexReader = blocks.sortedIndexReader,
                valuesReader = blocks.valuesReader
              ).get

            got.get.key shouldBe keyValue.key
            got
        }
      }
    }
  }
}
