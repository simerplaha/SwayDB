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

package swaydb.core.segment.format.one

import swaydb.compression.CompressionInternal
import swaydb.core.TestBase
import swaydb.core.actor.Actor
import swaydb.core.data._
import swaydb.core.io.reader.Reader
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.retry.Retry
import swaydb.core.util.TryUtil
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

/**
  * [[swaydb.core.group.compression.GroupCompressor]] is always invoked directly from [[Transient.Group]] there these test cases initialise the Group
  * to get full code coverage.
  *
  */
class GroupDecompressorSpec extends TestBase {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  val keyValueCount = 10000

  "GroupDecompressor" should {
    "Concurrently read multiple key-values" in {
      //only 100.bytes (very small) for key-values bytes so that all key-values get dropped from the cache eventually.
      implicit val keyValueLimiter = KeyValueLimiter(1.byte, 1.second)
      runThis(10.times) {
        //randomly generate key-values
        val keyValues =
          eitherOne(
            left = randomIntKeyValues(keyValueCount),
            right = randomizedIntKeyValues(keyValueCount)
          )

        //create a group for the key-values
        val group =
          Transient.Group(
            keyValues = keyValues,
            indexCompressions = Seq(randomCompression()),
            valueCompressions = Seq(randomCompression()),
            falsePositiveRate = 0.1,
            previous = None
          ).assertGet

        //write the group to a Segment
        val (bytes, _) = SegmentWriter.write(Seq(group), 0.1).assertGet

        //read footer
        val readKeyValues = SegmentReader.readAll(SegmentReader.readFooter(Reader(bytes)).assertGet, Reader(bytes)).assertGet
        readKeyValues should have size 1
        val persistentGroup = readKeyValues.head.asInstanceOf[Persistent.Group]

        //concurrently with 100 threads read randomly all key-values from the Group.
        Future.sequence {
          (1 to 100) map {
            _ =>
              Future {
                /**
                  * Reduce the number of retries in [[swaydb.core.group.compression.GroupDecompressor.maxTimesToTryDecompress]]
                  * to see [[Retry]] in this test perform retries.
                  *
                  * Also disable pattern matching for exceptions relating to Groups from [[swaydb.core.util.ExceptionUtil.logFailure]]
                  * to see Retries' log outputs by this test.
                  */
                Retry(resourceId = randomIntMax().toString, maxRetryLimit = 1000, until = Retry.levelReadRetryUntil) {
                  eitherOne(
                    left = Random.shuffle(unzipGroups(keyValues).toList),
                    right = unzipGroups(keyValues)
                  ) tryMap {
                    keyValue =>
                      persistentGroup.segmentCache.get(keyValue.key) match {
                        case Failure(exception) =>
                          Failure(exception)

                        case Success(value) =>
                          try {
                            value.get.toMemory shouldBe keyValue
                            TryUtil.successUnit
                          } catch {
                            case ex: Exception =>
                              Failure(ex.getCause)
                          }
                      }
                  }
                }.assertGet
              }
          }
        }.await(10.seconds)

        println("Done reading.")
        //cache should eventually be empty.
        eventual(10.seconds) {
          persistentGroup.segmentCache.isCacheEmpty shouldBe true
        }
        println("Cache is empty")
      }
    }
  }
}