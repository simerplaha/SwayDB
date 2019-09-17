///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.entry
//
//import org.scalatest.WordSpec
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.Transient
//import swaydb.core.data.Value.{FromValue, RangeValue}
//import swaydb.core.segment.format.a.block._
//import swaydb.core.segment.format.a.entry.reader.EntryReader
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class RangeEntryReaderWriterSpec extends WordSpec {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  "write and read single Range entry" in {
//    runThisParallel(1000.times) {
//      val fromKey = randomIntMax()
//      val toKey = randomIntMax() max (fromKey + 100)
//      val entry = randomRangeKeyValue(from = fromKey, to = toKey, randomFromValueOption()(TestTimer.random), randomRangeValue()(TestTimer.random)).toTransient
//      //      println("write: " + entry)
//
//      val read =
//        EntryReader.fullRead(
//          indexEntry = entry.indexEntryBytes.dropIntUnsigned().right.value,
//          mightBeCompressed = entry.stats.hasPrefixCompression,
//          valueCache = entry.valueEntryBytes.headOption.map(buildSingleValueCache),
//          indexOffset = 0,
//          nextIndexOffset = 0,
//          nextIndexSize = 0,
//          isNormalised = false,
//          hasAccessPositionIndex = entry.sortedIndexConfig.enableAccessPositionIndex,
//          previous = None
//        ).runRandomIO.right.value
//
//      //      println("read:  " + read)
//      read shouldBe entry
//    }
//  }
//
//  "write and read range entry with other entries" in {
//    runThisParallel(1000.times) {
//      val keyValues = randomizedKeyValues(count = 1, addPut = true)
//      val previous = keyValues.head
//
//      val fromKey = keyValues.last.key.readInt() + 1
//      val toKey = randomIntMax() max (fromKey + 100)
//      val next =
//        Transient.Range[FromValue, RangeValue](
//          fromKey = fromKey,
//          toKey = toKey,
//          fromValue = randomFromValueOption()(TestTimer.random),
//          rangeValue = randomRangeValue()(TestTimer.random),
//          valuesConfig = ValuesBlock.Config.random,
//          sortedIndexConfig = SortedIndexBlock.Config.random,
//          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
//          hashIndexConfig = HashIndexBlock.Config.random,
//          bloomFilterConfig = BloomFilterBlock.Config.random,
//          previous = Some(previous)
//        )
//
//      //      println("previous: " + previous)
//      //      println("next: " + next)
//
//      val valueBytes = (previous.valueEntryBytes ++ next.valueEntryBytes).flatten.toSlice
//
//      val previousRead =
//        EntryReader.fullRead(
//          indexEntry = previous.indexEntryBytes.dropIntUnsigned().right.value,
//          mightBeCompressed = false,
//          isNormalised = false,
//          valueCache = Some(buildSingleValueCache(valueBytes)),
//          indexOffset = 0,
//          nextIndexOffset = 0,
//          nextIndexSize = 0,
//          hasAccessPositionIndex = previous.sortedIndexConfig.enableAccessPositionIndex,
//          previous = None
//        ).runRandomIO.right.value
//
//      previousRead shouldBe previous
//
//      val nextRead =
//        EntryReader.fullRead(
//          indexEntry = next.indexEntryBytes.dropIntUnsigned().right.value,
//          mightBeCompressed = next.stats.hasPrefixCompression,
//          isNormalised = false,
//          valueCache = Some(buildSingleValueCache(valueBytes)),
//          indexOffset = 0,
//          nextIndexOffset = 0,
//          nextIndexSize = 0,
//          hasAccessPositionIndex = next.sortedIndexConfig.enableAccessPositionIndex,
//          previous = Some(previousRead)
//        ).runRandomIO.right.value
//
//      nextRead shouldBe next
//    }
//  }
//}
