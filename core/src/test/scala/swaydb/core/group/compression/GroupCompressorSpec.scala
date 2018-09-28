/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.group.compression

import swaydb.compression.CompressionInternal
import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

/**
  * [[swaydb.core.group.compression.GroupCompressor]] is always invoked directly from [[Transient.Group]] there these test cases initialise the Group
  * to get full code coverage.
  *
  */
class GroupCompressorSpec extends TestBase {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  val keyValueCount = 100

  "GroupCompressor" should {
    "return no Group if key-values are empty" in {
      Transient.Group(
        keyValues = Seq(),
        indexCompression = randomCompressionLZ4OrSnappy(Random.nextInt()),
        valueCompression = randomCompressionLZ4OrSnappy(Random.nextInt()),
        falsePositiveRate = 0.1,
        previous = None
      ).assertGetOpt shouldBe empty
    }

    "create a group on single key-value" when {
      "compression does not satisfy min compression requirement" in {
        runThis(10.times) {
          val keyValue =
            eitherOne(
              randomFixedKeyValue(1, eitherOne(None, Some(2))),
              randomRangeKeyValue(1, 2, randomFromValueOption(), rangeValue = Value.Update(2, randomDeadlineOption))
            )

          //println("Testing for key-values: " + keyValue)

          Transient.Group(
            keyValues = Seq(keyValue).toTransient,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = 0.1,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {
          val keyValue =
            eitherOne(
              randomFixedKeyValue("12345" * 20, eitherOne(None, Some("12345" * 20))),
              randomRangeKeyValue("12345", "12345" * 20, randomFromValueOption(), rangeValue = Value.Update("12345" * 30, randomDeadlineOption))
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
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValue.isInstanceOf[Memory.Range] || keyValue.getOrFetchValue.assertGetOpt.isDefined)
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
          val keyValues = randomIntKeyValues(keyValueCount)

          Transient.Group(
            keyValues = keyValues,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = 0.1,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {
          val keyValues =
            eitherOne(
              left = randomIntKeyValues(keyValueCount),
              right = randomizedIntKeyValues(keyValueCount)
            )

          val indexCompression = randomCompression()
          val valuesCompression = randomCompression()

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompression = indexCompression,
                valueCompression = valuesCompression,
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.assertGetOpt.isDefined))
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
              left = randomIntKeyValues(keyValueCount),
              right = randomizedIntKeyValues(keyValueCount)
            )

          val indexCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)
          val valueCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompressions = indexCompressions,
                valueCompressions = valueCompressions,
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompressions.last,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.assertGetOpt.isDefined))
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
              keyValues = randomIntKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              previous = None,
              falsePositiveRate = 0.1
            ).assertGet

          //add more key-values to existing group.
          val keyValues: Slice[KeyValue.WriteOnly] =
            (Seq(existingGroup) ++ randomIntKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats

          //create a new Group from key-values that already has an existing Group.
          Transient.Group(
            keyValues = keyValues,
            indexCompression = randomCompressionLZ4OrSnappy(12),
            valueCompression = randomCompressionLZ4OrSnappy(12),
            falsePositiveRate = 0.1,
            previous = None
          ).assertGetOpt shouldBe empty
        }
      }

      "compression satisfies min compression requirement" in {
        runThis(10.times) {

          //create an exiting Group
          val existingGroup =
            Transient.Group(
              keyValues = randomIntKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              falsePositiveRate = 0.1,
              previous = None
            ).assertGet

          //add more key-values to existing group.
          val keyValues: Slice[KeyValue.WriteOnly] =
            eitherOne(
              left = (Seq(existingGroup) ++ randomIntKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats,
              right = (Seq(existingGroup) ++ randomizedIntKeyValues(keyValueCount, startId = Some(existingGroup.keyValues.last.key.readInt() + 100000))).updateStats
            )

          val indexCompression = randomCompression()
          val valueCompression = randomCompression()

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompression = indexCompression,
                valueCompression = valueCompression,
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompression,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.assertGetOpt.isDefined))
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
              left = randomIntKeyValues(keyValueCount),
              right = randomizedIntKeyValues(keyValueCount)
            )

          val indexCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)
          val valueCompressions = Seq(randomCompressionLZ4OrSnappy(80), randomCompressionLZ4OrSnappy(60), CompressionInternal.UnCompressedGroup)

          assertGroup(
            group =
              Transient.Group(
                keyValues = keyValues,
                indexCompressions = indexCompressions,
                valueCompressions = valueCompressions,
                falsePositiveRate = 0.1,
                previous = None
              ).assertGet,
            expectedIndexCompressionUsed = indexCompressions.last,
            expectedValueCompressionUsed =
              //if either a Range of if the value is not None, then the compression will be used.
              if (keyValues.exists(keyValue => keyValue.isRange || keyValue.getOrFetchValue.assertGetOpt.isDefined))
                valueCompressions.lastOption
              else
                None
          )
        }
      }
    }
  }

}