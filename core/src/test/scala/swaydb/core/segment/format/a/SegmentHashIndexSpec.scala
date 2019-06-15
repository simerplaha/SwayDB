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

package swaydb.core.segment.format.a

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.io.reader.Reader
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder

class SegmentHashIndexSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  import keyOrder._

  "it" should {
    "build index" in {
      runThis(100.times) {
        val maxProbe = 10
        val keyValues = unzipGroups(randomizedKeyValues(count = 1000, startId = Some(1))).toMemory.toTransient

        val writeResult =
          SegmentHashIndex.write(
            keyValues = keyValues,
            maxProbe = maxProbe,
            minimumNumberOfKeyValues = 0,
            compensate = _ * 2
          ).get

        println(s"hit: ${writeResult.hit}")
        println(s"miss: ${writeResult.miss}")
        println

        writeResult.hit should be >= (keyValues.size * 0.50).toInt
        writeResult.miss shouldBe keyValues.size - writeResult.hit
        writeResult.hit + writeResult.miss shouldBe keyValues.size

        val indexOffsetMap: Map[Int, Transient] =
          keyValues.map({
            keyValue =>
              (keyValue.stats.thisKeyValuesHashIndexOffset, keyValue)
          })(collection.breakOut)

        def findKey(indexOffset: Int): IO[Option[Transient]] =
          IO(indexOffsetMap.get(indexOffset))

        val readResult =
          keyValues mapIO {
            keyValue =>
              SegmentHashIndex.find(
                key = keyValue.key,
                hashIndexReader = Reader(writeResult.bytes),
                hashIndexSize = writeResult.bytes.size,
                hashIndexStartOffset = 0,
                maxProbe = maxProbe,
                get = findKey,
                getNext = (_: Transient) => IO.none
              ) map {
                foundOption =>
                  foundOption map {
                    found =>
                      (found.key equiv keyValue.key) shouldBe true
                  }
              }
          }

        readResult.get.flatten.size should be >= writeResult.hit //>= because it can get lucky with overlapping index bytes.

        writeResult.bytes.moveWritePositionUnsafe(0)
        SegmentHashIndex.readHeader(Reader(writeResult.bytes)).get shouldBe
          SegmentHashIndex.Header(
            formatId = SegmentHashIndex.formatID,
            maxProbe = writeResult.maxProbe,
            hit = writeResult.hit,
            miss = writeResult.miss
          )
      }
    }
  }
}
