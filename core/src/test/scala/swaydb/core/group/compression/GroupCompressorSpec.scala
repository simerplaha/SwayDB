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

package swaydb.core.group.compression

import swaydb.compression.CompressionInternal
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.core.data._
import swaydb.data.slice.Slice
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import scala.util.Random

/**
  * [[swaydb.core.group.compression.GroupCompressor]] is always invoked directly from [[Transient.Group]] there these test cases initialise the Group
  * to get full code coverage.
  *
  */
class GroupCompressorSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.random

  val keyValueCount = 100

  "GroupCompressor" should {
    "return no Group if key-values are empty" in {
      Transient.Group(
        keyValues = Slice.empty,
        indexCompression = randomCompressionLZ4OrSnappy(Random.nextInt()),
        valueCompression = randomCompressionLZ4OrSnappy(Random.nextInt()),
        maxProbe = TestData.maxProbe,
        falsePositiveRate = TestData.falsePositiveRate,
        resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
        minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
        hashIndexCompensation = TestData.hashIndexCompensation,
        previous = None
      ).assertGetOpt shouldBe empty
    }

    "create a group on single key-value" when {
      "compression does not satisfy min compression requirement" in {
        runThis(10.times) {
          val keyValue =
            eitherOne(
              randomFixedKeyValue(1, eitherOne(None, Some(2))),
              randomRangeKeyValue(1, 2, randomFromValueOption(), rangeValue = Value.update(2, randomDeadlineOption))
            )

          //println("Testing for key-values: " + keyValue)

          Transient.Group(
            keyValues = Seq(keyValue).toTransient,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
            hashIndexCompensation = TestData.hashIndexCompensation,
            maxProbe = TestData.maxProbe,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {
          val keyValue =
            eitherOne(
              randomFixedKeyValue("12345" * 20, eitherOne(None, Some("12345" * 20))),
              randomRangeKeyValue("12345", "12345" * 20, randomFromValueOption(), rangeValue = Value.update("12345" * 30, randomDeadlineOption))
            )

          //println("Testing for key-values: " + keyValue)

          val indexCompression = randomCompressionLZ4OrSnappy(12)
          val valuesCompression = randomCompressionLZ4OrSnappy(12)

          assertGroup(
            group =
              Transient.Group(
                keyValues = Seq(keyValue).toTransient,
                indexCompression = indexCompression,
                valueCompression = valuesCompression,
                maxProbe = TestData.maxProbe,
                falsePositiveRate = TestData.falsePositiveRate,
                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
                hashIndexCompensation = TestData.hashIndexCompensation,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValue.isInstanceOf[Memory.Range] || keyValue.getOrFetchValue.isDefined)
                Some(valuesCompression)
              else
                None
          )
        }
      }
    }

    "create a group on multiple key-values" when {
      "compression does not satisfy min compression requirement" in {
        runThis(10.times) {
          val keyValues = randomKeyValues(keyValueCount)

          Transient.Group(
            keyValues = keyValues,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
            hashIndexCompensation = TestData.hashIndexCompensation,
            maxProbe = TestData.maxProbe,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {
          val keyValues =
            eitherOne(
              left = randomKeyValues(keyValueCount),
              right = randomizedKeyValues(keyValueCount)
            )

          val indexCompression = randomCompression()
          val valuesCompression = randomCompression()

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompression = indexCompression,
                valueCompression = valuesCompression,
                falsePositiveRate = TestData.falsePositiveRate,
                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
                hashIndexCompensation = TestData.hashIndexCompensation,
                maxProbe = TestData.maxProbe,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.isDefined))
                Some(valuesCompression)
              else
                None
          )
        }
      }

      "compression does not satisfies min compression requirement & the last compression is UnCompressedGroup" in {
        runThis(10.times) {
          val keyValues =
            eitherOne(
              left = randomKeyValues(keyValueCount),
              right = randomizedKeyValues(keyValueCount)
            )

          val indexCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)
          val valueCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompressions = indexCompressions,
                valueCompressions = valueCompressions,
                falsePositiveRate = TestData.falsePositiveRate,
                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
                hashIndexCompensation = TestData.hashIndexCompensation,
                maxProbe = TestData.maxProbe,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompressions.last,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.isDefined))
                Some(valueCompressions.last)
              else
                None
          )
        }
      }
    }

    "create a group on multiple Group key-values" when {
      "compression does not satisfy min compression requirement" in {
        runThis(10.times) {
          //create an exiting Group
          val existingGroup =
            Transient.Group(
              keyValues = randomKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              previous = None,
              maxProbe = TestData.maxProbe,
              falsePositiveRate = TestData.falsePositiveRate,
              resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
              minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
              hashIndexCompensation = TestData.hashIndexCompensation
            ).assertGet

          //add more key-values to existing group.
          val keyValues: Slice[KeyValue.WriteOnly] =
            (Seq(existingGroup) ++ randomKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats

          //create a new Group from key-values that already has an existing Group.
          Transient.Group(
            keyValues = keyValues,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
            hashIndexCompensation = TestData.hashIndexCompensation,
            maxProbe = TestData.maxProbe,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {

          //create an exiting Group
          val existingGroup =
            Transient.Group(
              keyValues = randomKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              falsePositiveRate = TestData.falsePositiveRate,
              resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
              minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
              hashIndexCompensation = TestData.hashIndexCompensation,
              maxProbe = TestData.maxProbe,
              previous = None
            ).assertGet

          //add more key-values to existing group.
          val keyValues: Slice[KeyValue.WriteOnly] =
            eitherOne(
              left = (Seq(existingGroup) ++ randomKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats,
              right = (Seq(existingGroup) ++ randomizedKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats
            )

          val indexCompression = randomCompression()
          val valueCompression = randomCompression()

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompression = indexCompression,
                valueCompression = valueCompression,
                falsePositiveRate = TestData.falsePositiveRate,
                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
                hashIndexCompensation = TestData.hashIndexCompensation,
                maxProbe = TestData.maxProbe,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.isDefined))
                Some(valueCompression)
              else
                None
          )
        }
      }

      "compression does not satisfies min compression requirement & the last compression is UnCompressedGroup" in {
        runThis(10.times) {
          val keyValues =
            eitherOne(
              left = randomKeyValues(keyValueCount),
              right = randomizedKeyValues(keyValueCount)
            )

          val indexCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)
          val valueCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompressions = indexCompressions,
                valueCompressions = valueCompressions,
                falsePositiveRate = TestData.falsePositiveRate,
                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
                hashIndexCompensation = TestData.hashIndexCompensation,
                maxProbe = TestData.maxProbe,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompressions.last,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.isDefined))
                valueCompressions.lastOption
              else
                None
          )
        }
      }
    }
  }

}
