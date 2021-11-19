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

package swaydb.core.segment.block.bloomfilter

import org.scalatest.OptionValues._
import swaydb.core.TestData._
import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.block.{Block, BlockCache}
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

import scala.util.Random

class BloomFilterBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  val keyValueCount = 1000

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            val compressions = eitherOne(Seq.empty, randomCompressions())

            val filter =
              BloomFilterBlock.init(
                numberOfKeys = 10,
                updateMaxProbe = probe => probe,
                falsePositiveRate = 0.01,
                compressions = _ => compressions
              ).value

            (1 to 10) foreach (BloomFilterBlock.add(_, filter))

            BloomFilterBlock.close(filter).value

            val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

            Seq(
              BlockRefReader[BloomFilterBlockOffset](filter.blockBytes),
              BlockRefReader[BloomFilterBlockOffset](createRandomFileReader(filter.blockBytes), blockCache)
            ) foreach {
              reader =>
                val bloom = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](reader)
                (1 to 10) foreach (key => BloomFilterBlock.mightContain(key, bloom) shouldBe true)
                (11 to 20) foreach (key => BloomFilterBlock.mightContain(key, bloom) shouldBe false)

              //println("numberOfBits: " + filter.numberOfBits)
              //println("written: " + filter.written)
            }
        }
      }
    }

    "write bloom filter to bytes for a single key-value" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            val compressions = eitherOne(Seq.empty, randomCompressions())

            val state =
              BloomFilterBlock.init(
                numberOfKeys = 10,
                updateMaxProbe = probe => probe,
                falsePositiveRate = 0.01,
                compressions = _ => compressions
              ).value

            BloomFilterBlock.add(1, state)

            BloomFilterBlock.close(state).value

            val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

            Seq(
              BlockRefReader[BloomFilterBlockOffset](state.blockBytes),
              BlockRefReader[BloomFilterBlockOffset](createRandomFileReader(state.blockBytes), blockCache)
            ) foreach {
              reader =>
                val bloom = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](reader)
                BloomFilterBlock.mightContain(1, bloom) shouldBe true
                (2 to 20) foreach (key => BloomFilterBlock.mightContain(key, bloom) shouldBe false)

              //println("numberOfBits: " + state.numberOfBits)
              //println("written: " + state.written)
            }
        }
      }
    }
  }

  "optimalSegmentBloomFilterByteSize" should {
    "return empty if false positive rate is 0.0 or number of keys is 0" in {
      BloomFilterBlock.optimalSize(1000, 0.0, randomBoolean(), updateMaxProbe = probe => probe, minimumNumberOfKeys = 0) shouldBe 0
      BloomFilterBlock.optimalSize(0, 0.001, randomBoolean(), updateMaxProbe = probe => probe, minimumNumberOfKeys = 0) shouldBe 0
    }

    "return the number of bytes required to store the Bloom filter" in {
      (1 to 1000) foreach {
        i =>
          val compressions = eitherOne(Seq.empty, randomCompressions())

          val numberOfItems = i * 10
          val falsePositiveRate = 0.0 + (0 + "." + i.toString).toDouble
          val compression = compressions

          val bloomFilter =
            BloomFilterBlock.init(
              numberOfKeys = numberOfItems,
              updateMaxProbe = probe => probe,
              falsePositiveRate = falsePositiveRate,
              compressions = _ => compression
            ).value

          bloomFilter.compressibleBytes.size should be <=
            BloomFilterBlock.optimalSize(
              numberOfKeys = numberOfItems,
              updateMaxProbe = probe => probe,
              falsePositiveRate = falsePositiveRate,
              hasCompression = compression.nonEmpty,
              minimumNumberOfKeys = 0
            )
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilterBlock.init(
        numberOfKeys = 0,
        updateMaxProbe = probe => probe,
        falsePositiveRate = randomFalsePositiveRate(),
        compressions = _ => randomCompressionsOrEmpty()
      ) shouldBe empty
    }

    "not initialise if false positive range is 0.0 are empty" in {
      BloomFilterBlock.init(
        numberOfKeys = 100,
        updateMaxProbe = probe => probe,
        falsePositiveRate = 0.0,
        compressions = _ => randomCompressionsOrEmpty()
      ) shouldBe empty
    }

    //    "not initialise bloomFilter if it contain removeRange" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        BloomFilterBlock.init(
    //          keyValues = KeyValueMergeBuilder.persistent(Slice(Memory.Range(1, 2, None, Value.Remove(None, time.next)))),
    //          config = BloomFilterConfig.random
    //        ) shouldBe empty
    //
    //        BloomFilterBlock.init(
    //          keyValues = KeyValueMergeBuilder.persistent(Slice(Memory.Range(1, 2, None, Value.Remove(Some(randomDeadline()), time.next)))),
    //          config = BloomFilterConfig.random
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "not initialise bloomFilter if it contains a range function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //range functions can also contain Remove so BloomFilter should not be created
    //        BloomFilterBlock.init(
    //          keyValues = KeyValueMergeBuilder.persistent(Slice(Memory.Range(1, 2, None, Value.Function(Slice.emptyBytes, time.next)))),
    //          config = BloomFilterConfig.random
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //range functions can also contain Remove so BloomFilter should not be created
    //        BloomFilterBlock.init(
    //          keyValues = KeyValueMergeBuilder.persistent(Slice(Memory.Range(1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next)))))),
    //          config = BloomFilterConfig.random
    //        ) shouldBe empty
    //
    //        BloomFilterBlock.init(
    //          keyValues = KeyValueMergeBuilder.persistent(Slice(Memory.Range(1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next)))))),
    //          config = BloomFilterConfig.random
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //pending apply should allow to create bloomFilter if it does not have remove or function.
    //        BloomFilterBlock.init(
    //          keyValues =
    //            KeyValueMergeBuilder.persistent(
    //              Slice(
    //                Memory.Range(
    //                  fromKey = 1,
    //                  toKey = 2,
    //                  fromValue = None,
    //                  rangeValue = Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next)))
    //                )
    //              )
    //            ),
    //          config = BloomFilterConfig.random.copy(falsePositiveRate = 0.001, minimumNumberOfKeys = 0)
    //        ) shouldBe defined
    //      }
    //    }

    //    "initialise bloomFilter when from value is remove but range value is not" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //        //fromValue is remove but it's not a remove range.
    //        BloomFilterBlock.init(
    //          keyValues =
    //            KeyValueMergeBuilder.persistent(
    //              Slice(
    //                Memory.Range(
    //                  fromKey = 1,
    //                  toKey = 2,
    //                  fromValue = Some(Value.Remove(None, time.next)),
    //                  rangeValue = Value.update(100)
    //                )
    //              )
    //            ),
    //          config = BloomFilterConfig.random.copy(falsePositiveRate = 0.001, minimumNumberOfKeys = 0)
    //        ) shouldBe defined
    //      }
    //    }
  }

  "bloomFilter error check" in {
    def runAssert(data: Seq[String],
                  reader: UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]) = {

      val positives =
        data collect {
          case data if !BloomFilterBlock.mightContain(data, reader.copy()) =>
            data
        }

      val falsePositives =
        data collect {
          case data if BloomFilterBlock.mightContain(Random.alphanumeric.take(2000).mkString.getBytes(), reader.copy()) =>
            data
        }

      println(s"errors out of ${data.size}: " + positives.size)
      println(s"falsePositives out of ${data.size}: " + falsePositives.size)
      println(s"Optimal byte size: " + reader.block.numberOfBits)
      println(s"Actual byte size: " + reader.block.offset.size)
      println

      positives.size shouldBe 0
      falsePositives.size should be < 200
    }

    runThis(5.times) {
      TestCaseSweeper {
        implicit sweeper =>
          val compressions = eitherOne(Seq.empty, randomCompressions())

          val state =
            BloomFilterBlock.init(
              numberOfKeys = 10000,
              updateMaxProbe = probe => probe,
              falsePositiveRate = 0.001,
              compressions = _ => compressions
            ).value

          val data: Seq[String] =
            (1 to 10000) map {
              _ =>
                val string = Random.alphanumeric.take(2000).mkString
                BloomFilterBlock.add(string.getBytes(), state)
                string
            }

          BloomFilterBlock.close(state).value

          runAssert(
            data = data,
            reader = BloomFilterBlock.unblockedReader(state)
          )

          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

          Seq(
            BlockRefReader[BloomFilterBlockOffset](state.blockBytes),
            BlockRefReader[BloomFilterBlockOffset](createRandomFileReader(state.blockBytes), blockCache)
          ) foreach {
            blockRefReader =>
              val reader = Block.unblock[BloomFilterBlockOffset, BloomFilterBlock](blockRefReader)
              runAssert(
                data = data,
                reader = reader
              )
          }
      }
    }
  }
}
