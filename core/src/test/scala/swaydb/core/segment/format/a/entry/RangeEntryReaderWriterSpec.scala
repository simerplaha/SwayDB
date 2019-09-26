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

package swaydb.core.segment.format.a.entry

import org.scalatest.WordSpec
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.Transient
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class RangeEntryReaderWriterSpec extends WordSpec {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  "write and read single Range entry" in {
    runThisParallel(1000.times) {
      val fromKey = randomIntMax()
      val toKey = randomIntMax() max (fromKey + 100)
      val entry = randomRangeKeyValue(from = fromKey, to = toKey, randomFromValueOption()(TestTimer.random), randomRangeValue()(TestTimer.random)).toTransient
      //      println("write: " + entry)

      //if normalise is true, use normalised entry.
      val normalisedEntry =
        if (entry.sortedIndexConfig.normaliseIndex)
          Transient.normalise(Slice(entry)).head
        else
          Slice(entry).head

      val read =
        EntryReader.fullRead(
          isPartialReadEnabled = entry.sortedIndexConfig.enablePartialRead,
          indexEntry = normalisedEntry.indexEntryBytes.dropUnsignedInt(),
          mightBeCompressed = entry.stats.hasPrefixCompression,
          valuesReader = entry.valueEntryBytes.map(buildSingleValueReader),
          indexOffset = 0,
          nextIndexOffset = 0,
          nextIndexSize = 0,
          hasAccessPositionIndex = entry.sortedIndexConfig.enableAccessPositionIndex,
          isNormalised = entry.sortedIndexConfig.normaliseIndex,
          previous = None
        ).runRandomIO.right.value

      //      println("read:  " + read)
      read shouldBe entry
    }
  }

  "write and read range entry with other entries" in {
    runThisParallel(1000.times) {
      val previousNotNormalised = randomizedKeyValues(count = 1).head

      val fromKey = previousNotNormalised.key.readInt() + 1
      val toKey = randomIntMax() max (fromKey + 100)
      val nextNotNormalised =
        Transient.Range[FromValue, RangeValue](
          fromKey = fromKey,
          toKey = toKey,
          fromValue = randomFromValueOption()(TestTimer.random),
          rangeValue = randomRangeValue()(TestTimer.random),
          valuesConfig = previousNotNormalised.valuesConfig,
          sortedIndexConfig = previousNotNormalised.sortedIndexConfig,
          binarySearchIndexConfig = previousNotNormalised.binarySearchIndexConfig,
          hashIndexConfig = previousNotNormalised.hashIndexConfig,
          bloomFilterConfig = previousNotNormalised.bloomFilterConfig,
          previous = Some(previousNotNormalised)
        )

      val (previous: Transient, next: Transient) =
        if (nextNotNormalised.sortedIndexConfig.normaliseIndex) {
          val normalised = Transient.normalise(Slice(previousNotNormalised, nextNotNormalised))
          (normalised.head, normalised.last)
        } else {
          (previousNotNormalised, nextNotNormalised)
        }

      //      println
      //      println("write previous: " + previous)
      //      println("write next: " + next)

      val valueBytes: Slice[Byte] = previous.valueEntryBytes ++ next.valueEntryBytes

      val previousRead =
        EntryReader.fullRead(
          isPartialReadEnabled = next.sortedIndexConfig.enablePartialRead,
          indexEntry = previous.indexEntryBytes.dropUnsignedInt(),
          mightBeCompressed = next.stats.hasPrefixCompression,
          valuesReader = Some(buildSingleValueReader(valueBytes)),
          indexOffset = 0,
          nextIndexOffset = 0,
          nextIndexSize = 0,
          hasAccessPositionIndex = next.sortedIndexConfig.enableAccessPositionIndex,
          isNormalised = next.sortedIndexConfig.normaliseIndex,
          previous = None
        ).runRandomIO.right.value

      previousRead shouldBe previous

      val nextRead =
        EntryReader.fullRead(
          isPartialReadEnabled = next.sortedIndexConfig.enablePartialRead,
          indexEntry = next.indexEntryBytes.dropUnsignedInt(),
          mightBeCompressed = next.stats.hasPrefixCompression,
          valuesReader = Some(buildSingleValueReader(valueBytes)),
          indexOffset = 0,
          nextIndexOffset = 0,
          nextIndexSize = 0,
          hasAccessPositionIndex = next.sortedIndexConfig.enableAccessPositionIndex,
          isNormalised = next.sortedIndexConfig.normaliseIndex,
          previous = Some(previousRead)
        ).runRandomIO.right.value

      //      val nextRead = EntryReader.read(Reader(next.indexEntryBytes), Reader(valueBytes), 0, 0, 0, Some(previousRead)).runIO
      nextRead shouldBe next

      //      println("read previous:  " + previousRead)
      //      println("read next:  " + nextRead)
      //      println
    }
  }
}
