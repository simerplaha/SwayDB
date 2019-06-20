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

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.index.HashIndex
import swaydb.core.util.CollectionUtil._
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable

class HashIndexSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  import keyOrder._

  "it" should {
    "build index" when {
      "there are only fixed key-values" in {
        runThis(100.times) {
          val maxProbe = 5
          //        val keyValues = unzipGroups(randomizedKeyValues(count = 1000, startId = Some(1), addRandomGroups = false, addRandomRanges = false)).toMemory.toTransient
          val keyValues = randomKeyValues(1000, startId = Some(1), addRandomRemoves = true, addRandomFunctions = true, addRandomRemoveDeadlines = true, addRandomUpdates = true, addRandomPendingApply = true)

          val bytesSize =
            HashIndex.optimalBytesRequired(
              keyCounts = keyValues.last.stats.segmentUniqueKeysCount,
              largestValue = keyValues.last.stats.thisKeyValuesAccessIndexOffset,
              compensate = _ => 0
            )

          val bytes = Slice.create[Byte](bytesSize)

          val writeState =
            keyValues.foldLeftIO(HashIndex.State(0, 0, maxProbe, bytes)) {
              case (state, keyValue) =>
                HashIndex.write(
                  key = keyValue.key,
                  value = keyValue.stats.thisKeyValuesAccessIndexOffset,
                  state = state
                ) map {
                  _ =>
                    state
                }
            }.get

          println(s"hit: ${writeState.hit}")
          println(s"miss: ${writeState.miss}")
          println

          writeState.hit should be >= (keyValues.size * 0.50).toInt
          writeState.miss shouldBe keyValues.size - writeState.hit
          writeState.hit + writeState.miss shouldBe keyValues.size

          val indexOffsetMap: mutable.ListMap[Int, Transient] =
            keyValues.map({
              keyValue =>
                (keyValue.stats.thisKeyValuesAccessIndexOffset, keyValue)
            })(collection.breakOut)

          def findKey(indexOffset: Int, key: Slice[Byte]): IO[Option[Transient]] =
            indexOffsetMap.get(indexOffset) match {
              case some @ Some(found) if found.key equiv key =>
                IO.Success(some) //woohoo! Found at first go.

              case Some(_) =>
                IO {
                  //if not found collect the next block of prefix compress key-values
                  //and see if the key exists in them.
                  var found = Option.empty[Transient]
                  indexOffsetMap.iterator foreachBreak {
                    case (index, keyValue) if index > indexOffset => //walk upto the index.
                      //ones we've reached the index, check for the index in the next compression block.
                      if (keyValue.enablePrefixCompression) {
                        //in the next prefix compression block
                        if (keyValue.key equiv key) {
                          found = Some(keyValue)
                          true
                        } else {
                          false //moved out of the prefix compression block. kill!
                        }
                      } else {
                        true
                      }

                    case _ =>
                      false
                  }
                  found
                }

              case None =>
                IO.none
            }

          val readResult =
            keyValues mapIO {
              keyValue =>
                HashIndex.find(
                  key = keyValue.key,
                  hashIndexReader = Reader(writeState.bytes),
                  hashIndexSize = writeState.bytes.size,
                  hashIndexStartOffset = 0,
                  maxProbe = maxProbe,
                  assertValue = findKey(_, keyValue.key)
                ) map {
                  foundOption =>
                    foundOption map {
                      found =>
                        (found.key equiv keyValue.key) shouldBe true
                    }
                }
            }

          readResult.get.flatten.size should be >= writeState.hit //>= because it can getFromHashIndex lucky with overlapping index bytes.

          HashIndex.writeHeader(writeState).get

          HashIndex.readHeader(Reader(writeState.bytes)).get shouldBe
            HashIndex.Header(
              formatId = HashIndex.formatID,
              maxProbe = writeState.maxProbe,
              hit = writeState.hit,
              miss = writeState.miss
            )
        }
      }
    }
  }
}
