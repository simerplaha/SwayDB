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

package swaydb.core

import java.nio.file.Paths
import java.util.concurrent.ConcurrentSkipListMap

import org.scalatest.exceptions.TestFailedException
import swaydb.compression.CompressionInternal
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.KeyValue.{ReadOnly, WriteOnly}
import swaydb.core.data.Memory.PendingApply
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.group.compression.GroupCompressor
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.io.file.IOEffect
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.level.zero.{LevelZero, LevelZeroSkipListMerger}
import swaydb.core.level.{Level, LevelRef, NextLevel}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryWriter, RangeValueSerializer, ValueSerializer}
import swaydb.core.merge._
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.{KeyMatcher, SegmentSearcher}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.CollectionUtil._
import swaydb.data.IO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.StorageUnits._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

object CommonAssertions {

  implicit class RunSafe[T](input: => T) {
    def safeGetBlocking(): T =
      IO.Async.runSafe(input).safeGetBlocking.get
  }

  implicit class KeyValueImplicits(actual: KeyValue) {

    def asPut: Option[KeyValue.ReadOnly.Put] =
      actual match {
        case keyValue: KeyValue.ReadOnly.Put =>
          Some(keyValue)

        case keyValue: Transient.Put =>
          Some(keyValue.toMemory.asInstanceOf[Memory.Put])

        case range: KeyValue.ReadOnly.Range =>
          range.fetchFromValue.assertGetOpt flatMap {
            case put: Value.Put =>
              Some(put.toMemory(range.fromKey))
            case _ =>
              None
          }

        case range: Transient.Range =>
          range.fetchFromValue.assertGetOpt flatMap {
            case put: Value.Put =>
              Some(put.toMemory(range.fromKey))
            case _ =>
              None
          }

        case _ =>
          None
      }

    def toMemory: Memory =
      actual match {
        case readOnly: ReadOnly => readOnly.toMemory
        case writeOnly: WriteOnly => writeOnly.toMemory
      }

    def toMemoryResponse: Memory.SegmentResponse =
      actual match {
        case readOnly: ReadOnly => readOnly.toMemoryResponse
        case writeOnly: WriteOnly => writeOnly.toMemoryResponse
      }

    def shouldBe(expected: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                     keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Unit = {
      val actualMemory = actual.toMemory
      val expectedMemory = expected.toMemory

      (actualMemory, expectedMemory) match {
        case (actual: Memory.Group, expected: Memory.Group) =>
          actual.segment.getAll().assertGet shouldBe expected.segment.getAll().assertGet
        case _ =>
          actualMemory should be(expectedMemory)
      }
    }

    def getOrFetchValue: Option[Slice[Byte]] =
      actual match {
        case keyValue: Memory =>
          keyValue match {
            case keyValue: Memory.Put =>
              keyValue.value
            case keyValue: Memory.Update =>
              keyValue.value
            case keyValue: Memory.Function =>
              Some(keyValue.getOrFetchFunction.get.safeGetBlocking())
            case keyValue: Memory.PendingApply =>
              val bytes = Slice.create[Byte](ValueSerializer.bytesRequired(keyValue.getOrFetchApplies.get.safeGetBlocking()))
              ValueSerializer.write(keyValue.getOrFetchApplies.get.safeGetBlocking())(bytes)
              Some(bytes)
            case keyValue: Memory.Remove =>
              None
            case keyValue: Memory.Range =>
              val bytes = Slice.create[Byte](RangeValueSerializer.bytesRequired(keyValue.fromValue, keyValue.rangeValue))
              RangeValueSerializer.write(keyValue.fromValue, keyValue.rangeValue)(bytes)
              Some(bytes)

            case keyValue: Memory.Group =>
              Option(keyValue.compressedKeyValues)
          }
        case keyValue: Transient =>
          keyValue match {
            case keyValue: Transient.Put =>
              keyValue.value
            case keyValue: Transient.Update =>
              keyValue.value
            case keyValue: Transient.Function =>
              Some(keyValue.function)
            case keyValue: Transient.PendingApply =>
              keyValue.value
            case keyValue: Transient.Remove =>
              keyValue.value
            case keyValue: Transient.Range =>
              keyValue.value
            case keyValue: Transient.Group =>
              Some(keyValue.result.flattenSegmentBytes)
          }
        case keyValue: Persistent =>
          keyValue match {
            case keyValue: Persistent.Put =>
              keyValue.getOrFetchValue.get.safeGetBlocking()
            case keyValue: Persistent.Update =>
              keyValue.getOrFetchValue.get.safeGetBlocking()
            case keyValue: Persistent.Function =>
              Some(keyValue.getOrFetchFunction.get.safeGetBlocking())
            case keyValue: Persistent.PendingApply =>
              keyValue.lazyValueReader.getOrFetchValue.get.safeGetBlocking()
            case keyValue: Persistent.Remove =>
              keyValue.getOrFetchValue
            case keyValue: Persistent.Range =>
              keyValue.lazyRangeValueReader.getOrFetchValue.get.safeGetBlocking()
            case keyValue: Persistent.Group =>
              keyValue
                .valueReader
                .moveTo(keyValue.valueOffset)
                .read(keyValue.valueLength)
                .safeGetBlocking()
                .map(Some(_))
                .get
          }
      }
  }

  def randomly[T](f: => T): Option[T] =
    if (Random.nextBoolean())
      Some(f)
    else
      None

  def eitherOne[T](left: => T, right: => T): T =
    if (Random.nextBoolean())
      left
    else
      right

  def anyOrder[T](left: => T, right: => T): Unit =
    if (Random.nextBoolean()) {
      left
      right
    } else {
      right
      left
    }

  def eitherOne[T](left: => T, mid: => T, right: => T): T =
    Random.shuffle(Seq(() => left, () => mid, () => right)).head()

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four)).head()

  def randomGroupingStrategyOption(keyValuesCount: Int): Option[KeyValueGroupingStrategyInternal] =
    eitherOne(
      left = None,
      right = Some(randomGroupingStrategy(keyValuesCount))
    )

  def randomGroupingStrategy(keyValuesCount: Int): KeyValueGroupingStrategyInternal =
    eitherOne(
      left =
        KeyValueGroupingStrategyInternal.Count(
          count = (keyValuesCount / (randomIntMax(50) + 1)) max 1000,
          groupCompression =
            eitherOne(
              left =
                Some(
                  GroupGroupingStrategyInternal.Count(
                    count = randomIntMax(5) max 1,
                    valuesConfig = Values.Config.random,
                    sortedIndexConfig = SortedIndex.Config.random,
                    binarySearchIndexConfig = BinarySearchIndex.Config.random,
                    hashIndexConfig = HashIndex.Config.random,
                    bloomFilterConfig = BloomFilter.Config.random,
                    groupCompressions = randomCompressions()
                  )
                ),
              mid =
                Some(
                  GroupGroupingStrategyInternal.Size(
                    size = randomIntMax(keyValuesCount max 1000).bytes * 2,
                    valuesConfig = Values.Config.random,
                    sortedIndexConfig = SortedIndex.Config.random,
                    binarySearchIndexConfig = BinarySearchIndex.Config.random,
                    hashIndexConfig = HashIndex.Config.random,
                    bloomFilterConfig = BloomFilter.Config.random,
                    groupCompressions = randomCompressions()
                  )
                ),
              right =
                None
            ),
          valuesConfig = Values.Config.random,
          sortedIndexConfig = SortedIndex.Config.random,
          binarySearchIndexConfig = BinarySearchIndex.Config.random,
          hashIndexConfig = HashIndex.Config.random,
          bloomFilterConfig = BloomFilter.Config.random,
          groupCompressions = randomCompressions(),
          applyGroupingOnCopy = randomBoolean()
        ),
      right =
        KeyValueGroupingStrategyInternal.Size(
          size = keyValuesCount.kb,
          groupCompression =
            eitherOne(
              left =
                Some(
                  GroupGroupingStrategyInternal.Count(
                    count = randomIntMax(5) max 1,
                    valuesConfig = Values.Config.random,
                    sortedIndexConfig = SortedIndex.Config.random,
                    binarySearchIndexConfig = BinarySearchIndex.Config.random,
                    hashIndexConfig = HashIndex.Config.random,
                    bloomFilterConfig = BloomFilter.Config.random,
                    groupCompressions = randomCompressions()
                  )
                ),
              mid =
                Some(
                  GroupGroupingStrategyInternal.Size(
                    size = randomIntMax(500).kb,
                    valuesConfig = Values.Config.random,
                    sortedIndexConfig = SortedIndex.Config.random,
                    binarySearchIndexConfig = BinarySearchIndex.Config.random,
                    hashIndexConfig = HashIndex.Config.random,
                    bloomFilterConfig = BloomFilter.Config.random,
                    groupCompressions = randomCompressions()
                  )),
              right =
                None
            ),
          valuesConfig = Values.Config.random,
          sortedIndexConfig = SortedIndex.Config.random,
          binarySearchIndexConfig = BinarySearchIndex.Config.random,
          hashIndexConfig = HashIndex.Config.random,
          bloomFilterConfig = BloomFilter.Config.random,
          groupCompressions = randomCompressions(),
          applyGroupingOnCopy = randomBoolean()
        )
    )

  implicit class ValueImplicits(value: Value) {

    @tailrec
    final def deadline: Option[Deadline] =
      value match {
        case value: FromValue =>
          value match {
            case value: Value.RangeValue =>
              value match {
                case Value.Remove(deadline, time) =>
                  deadline
                case Value.Update(value, deadline, time) =>
                  deadline
                case Value.Function(function, time) =>
                  None
                case pending: Value.PendingApply =>
                  pending.applies.last.deadline
              }
            case Value.Put(value, deadline, time) =>
              deadline
          }
      }
  }

  implicit class IsKeyValueExpectedInLastLevel(keyValue: Memory.Fixed) {
    def isExpectedInLastLevel: Boolean =
      keyValue match {
        case Memory.Put(key, value, deadline, time) =>
          if (deadline.forall(_.hasTimeLeft()))
            true
          else
            false
        case _: Memory.Update | _: Memory.Remove | _: Memory.Function | _: Memory.PendingApply =>
          false
      }
  }

  implicit class MemoryKeyValueImplicits(keyValue: Memory) {
    def toLastLevelExpected: Option[Memory.Fixed] =
      keyValue match {
        case expectedLevel: Memory.Put =>
          if (expectedLevel.hasTimeLeft())
            Some(expectedLevel)
          else
            None
        case range: Memory.Range =>
          range.fromValue flatMap {
            case range: Value.Put =>
              if (range.hasTimeLeft())
                Some(range.toMemory(keyValue.key))
              else
                None
            case _ =>
              None
          }
        case _: Memory.Update =>
          None
        case _: Memory.Function =>
          None
        case _: Memory.PendingApply =>
          None
        case _: Memory.Remove =>
          None
      }
  }

  implicit class PrintSkipList(skipList: ConcurrentSkipListMap[Slice[Byte], Memory]) {

    import swaydb.serializers.Default._
    import swaydb.serializers._

    //stringify the skipList so that it's readable
    def asString(value: Value): String =
      value match {
        case Value.Remove(deadline, time) =>
          s"Remove(deadline = $deadline)"
        case Value.Put(value, deadline, time) =>
          s"Put(${value.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
        case Value.Update(value, deadline, time) =>
          s"Update(${value.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
      }
  }

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly.SegmentResponse],
                          oldKeyValues: Iterable[KeyValue.ReadOnly.SegmentResponse],
                          expected: KeyValue.WriteOnly): ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected))

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly.SegmentResponse],
                          oldKeyValues: Iterable[KeyValue.ReadOnly.SegmentResponse],
                          expected: Iterable[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long): ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse] = {
    val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](KeyOrder.default)
    (oldKeyValues ++ newKeyValues).map(_.toMemoryResponse) foreach (memory => LevelZeroSkipListMerger.insert(memory.key, memory, skipList))
    skipList.size() shouldBe expected.size
    skipList.asScala.toList shouldBe expected.map(keyValue => (keyValue.key, keyValue.toMemory))
    skipList
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  oldKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  expected: Slice[KeyValue.WriteOnly],
                  isLastLevel: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  oldKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  expected: Slice[KeyValue.WriteOnly],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] = {
    val result =
      SegmentMerger.merge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        minSegmentSize = 10.mb,
        isLastLevel = isLastLevel,
        forInMemory = false,
        valuesConfig = Values.Config.random,
        sortedIndexConfig = SortedIndex.Config.random,
        binarySearchIndexConfig = BinarySearchIndex.Config.random,
        hashIndexConfig = HashIndex.Config.random,
        bloomFilterConfig = BloomFilter.Config.random
      ).assertGet

    if (expected.size == 0) {
      result shouldBe empty
    } else {
      result should have size 1
      val ungrouped = unzipGroups(result.head)
      ungrouped should have size expected.size
      ungrouped.toList should contain inOrderElementsOf expected
    }
    result
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  oldKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: KeyValue.ReadOnly)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), Slice(lastLevelExpect))

  def assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  oldKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: Option[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.map(Slice(_)).getOrElse(Slice.empty))
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected))
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  oldKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValues, oldKeyValues, expected.toTransient(), isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect.toTransient(), isLastLevel = true)
    //println("*** Skip list assert ***")
    assertSkipListMerge(newKeyValues, oldKeyValues, expected)
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  oldKeyValue: KeyValue.ReadOnly.SegmentResponse,
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected.toTransient(), isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect.toTransient(), isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  oldKeyValues: Slice[KeyValue.ReadOnly.SegmentResponse],
                  expected: KeyValue.WriteOnly,
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), isLastLevel)

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Remove,
                  oldKeyValue: Memory.Fixed,
                  expected: KeyValue.ReadOnly.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    RemoveMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Put,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    PutMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.PendingApply,
                  expected: KeyValue.ReadOnly.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Fixed,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.PendingApply,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    FixedMerger(newKeyValue, oldKeyValue).assertGet shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly.SegmentResponse, oldKeyValue: KeyValue.ReadOnly.SegmentResponse, expected, lastLevel)
    //todo merge with persistent
  }

  implicit class SliceKeyValueImplicits(actual: Iterable[KeyValue]) {
    def shouldBe(expected: Iterable[KeyValue]): Unit = {
      val unzipActual = unzipGroups(actual)
      val unzipExpected = unzipGroups(expected)
      unzipActual.size shouldBe unzipExpected.size
      unzipActual.zip(unzipExpected) foreach {
        case (left, right) =>
          left shouldBe right
      }
    }

    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.SegmentResponse]]) =
    //LevelZero does not write Groups therefore this unzip is required.
      unzipGroups(actual).foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.SegmentResponse]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory.SegmentResponse](keyValue.key, keyValue.toMemoryResponse)
          mapEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class MemoryImplicits(actual: Iterable[Memory.SegmentResponse]) {
    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.SegmentResponse]]) =
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.SegmentResponse]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory.SegmentResponse](keyValue.key, keyValue)
          mapEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class SegmentsImplicits(actual: Iterable[Segment]) {

    def shouldHaveSameKeyValuesAs(expected: Iterable[Segment]): Unit =
      Segment.getAllKeyValues(actual).assertGet shouldBe Segment.getAllKeyValues(expected).assertGet
  }

  implicit class SliceByteImplicits(actual: Slice[Byte]) {
    def shouldHaveSameKey(expected: KeyValue): Unit =
      actual shouldBe expected.key

    def shouldBeSliced() =
      actual.underlyingArraySize shouldBe actual.toArrayCopy.length
  }

  implicit class OptionSliceByteImplicits(actual: Option[Slice[Byte]]) {
    def shouldBeSliced() =
      actual foreach (_.shouldBeSliced())
  }

  def getStats(keyValue: KeyValue): Option[Stats] =
    keyValue match {
      case _: KeyValue.ReadOnly =>
        None
      case keyValue: KeyValue.WriteOnly =>
        Some(keyValue.stats)
    }

  implicit class StatsOptionImplicits(actual: Option[Stats]) {
    def shouldBe(expected: Option[Stats], ignoreValueOffset: Boolean = false) =
      if (actual.isDefined && expected.isDefined)
        actual.assertGet shouldBe(expected.assertGet, ignoreValueOffset)
  }

  implicit class PersistentReadOnlyOptionImplicits(actual: Option[Persistent]) {
    def shouldBe(expected: Option[Persistent]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }
  }

  implicit class PersistentReadOnlyKeyValueOptionImplicits(actual: Option[Persistent]) {
    def shouldBe(expected: Option[KeyValue.WriteOnly]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }

    def shouldBe(expected: KeyValue.WriteOnly) =
      actual.assertGet shouldBe expected
  }

  implicit class PersistentReadOnlyKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: KeyValue.WriteOnly) = {
      actual.toMemory shouldBe expected.toMemory
    }
  }

  implicit class PersistentReadOnlyImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) =
      actual.toMemory shouldBe expected.toMemory
  }

  implicit class SegmentImplicits(actual: Segment) {

    def shouldBe(expected: Segment): Unit = {
      actual.segmentSize shouldBe expected.segmentSize
      actual.minKey shouldBe expected.minKey
      actual.maxKey shouldBe expected.maxKey
      actual.existsOnDisk shouldBe expected.existsOnDisk
      actual.path shouldBe expected.path
      assertReads(expected.getAll().assertGet, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValue]): Unit =
      keyValues.foreach {
        keyValue =>
          actual.get(keyValue.key).assertGet shouldBe keyValue
      }
  }

  implicit class MapEntryImplicits(actual: MapEntry[Slice[Byte], Memory.SegmentResponse]) {

    def shouldBe(expected: MapEntry[Slice[Byte], Memory.SegmentResponse]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual.totalByteSize shouldBe expected.totalByteSize
      actual match {
        case MapEntry.Put(key, value) =>
          val exp = expected.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.SegmentResponse]]
          key shouldBe exp.key
          value shouldBe exp.value

        case MapEntry.Remove(key) =>
          val exp = expected.asInstanceOf[MapEntry.Remove[Slice[Byte]]]
          key shouldBe exp.key

        case _ => //MapEntry is a batch of other MapEntries, iterate and assert.
          expected.entries.size shouldBe actual.entries.size
          expected.entries.zip(actual.entries) foreach {
            case (expected, actual) =>
              actual shouldBe expected
          }
      }
    }
  }

  implicit class SegmentsPersistentMapImplicits(actual: MapEntry[Slice[Byte], Segment]) {

    def shouldBe(expected: MapEntry[Slice[Byte], Segment]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize

      val actualMap = new ConcurrentSkipListMap[Slice[Byte], Segment](KeyOrder.default)
      actual.applyTo(actualMap)

      val expectedMap = new ConcurrentSkipListMap[Slice[Byte], Segment](KeyOrder.default)
      expected.applyTo(expectedMap)

      actualMap.size shouldBe expectedMap.size

      val actualArray = actualMap.asScala.toArray
      val expectedArray = expectedMap.asScala.toArray

      actualArray.indices.foreach {
        i =>
          val actual = actualArray(i)
          val expected = expectedArray(i)
          actual._1 shouldBe expected._1
          actual._2 shouldBe expected._2
      }
    }
  }

  def assertHigher(keyValuesIterable: Iterable[KeyValue],
                   level: LevelRef): Unit = {
    val keyValues = keyValuesIterable.toSlice
    assertHigher(keyValues, getHigher = key => level.higher(key).safeGetBlocking)
  }

  def assertLower(keyValuesIterable: Iterable[KeyValue],
                  level: LevelRef) = {
    val keyValues = keyValuesIterable.toArray

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        level.lower(keyValues(0).key).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        try {
          val lower = level.lower(keyValues(index).key).assertGetOpt

          val expectedLowerKeyValue =
            (0 until index).reverse collectFirst {
              case i if unexpiredPuts(Slice(keyValues(i))).nonEmpty =>
                keyValues(i)
            }

          if (lower.nonEmpty) {
            expectedLowerKeyValue shouldBe defined
            lower.get.key shouldBe expectedLowerKeyValue.get.key
            lower.get.getOrFetchValue.get.safeGetBlocking shouldBe expectedLowerKeyValue.get.getOrFetchValue.safeGetBlocking()
          } else {
            expectedLowerKeyValue shouldBe empty
          }
        } catch {
          case exception: Exception =>
            exception.printStackTrace()
            fail(exception)
        }
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertGet(keyValues: Slice[KeyValue.WriteOnly],
                reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {
    val (footer, valuesReader, sortedIndex, hashIndex, binarySearchIndex, bloomFilter) = readBlocks(reader.copy()).get

    keyValues foreach {
      keyValue =>
        SegmentSearcher.get(
          key = keyValue.minKey,
          start = None,
          end = None,
          hashIndex = hashIndex,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          valuesReader = valuesReader,
          hasRange = footer.hasRange
        ).assertGet shouldBe keyValue
    }
  }

  def assertBloom(keyValues: Slice[KeyValue.WriteOnly],
                  bloom: BloomFilter.State) = {
    val unzipedKeyValues = unzipGroups(keyValues)
    val bloomFilter = BloomFilter.read(BloomFilter.Offset(0, bloom.startOffset), SegmentBlock.createUnblockedReader(bloom.bytes).get).get

    unzipedKeyValues.par.count {
      keyValue =>
        BloomFilter.mightContain(
          key = keyValue.key,
          reader = bloomFilter.createBlockReader(SegmentBlock.createUnblockedReader(bloom.bytes).get)
        ).get
    } should be >= (unzipedKeyValues.size * 0.90).toInt

    assertBloomNotContains(bloom)
  }

  def assertBloom(keyValues: Slice[KeyValue.WriteOnly],
                  segment: Segment) = {
    val unzipedKeyValues = unzipGroups(keyValues)

    unzipedKeyValues.par.count {
      keyValue =>
        segment.mightContainKey(keyValue.key).get
    } should be >= (unzipedKeyValues.size * 0.90).toInt

    assertBloomNotContains(segment)
  }

  def assertBloomNotContains(segment: Segment) =
    runThis(1000.times) {
      IO(segment.mightContainKey(randomBytesSlice(randomIntMax(1000) min 100)) shouldBe false)
    }

  def assertBloomNotContains(bloom: BloomFilter.State) =
    runThis(1000.times) {
      val bloomFilter = BloomFilter.read(BloomFilter.Offset(0, bloom.startOffset), SegmentBlock.createUnblockedReader(bloom.bytes).get).get
      BloomFilter.mightContain(
        key = randomBytesSlice(randomIntMax(1000) min 100),
        reader = bloomFilter.createBlockReader(SegmentBlock.createUnblockedReader(bloom.bytes).get)
      ).get shouldBe false
    }

  def assertReads(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertAllSegmentsCreatedInLevel(level: Level) =
    level.segmentsInLevel() foreach (_.createdInLevel.assertGet shouldBe level.levelNumber)

  def assertReads(keyValues: Iterable[KeyValue],
                  level: LevelRef) = {
    val asserts = Seq(() => assertGet(keyValues, level), () => assertHigher(keyValues, level), () => assertLower(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertNoneReads(keyValues: Iterable[KeyValue],
                      level: LevelRef) = {
    val asserts = Seq(() => assertGetNone(keyValues, level), () => assertHigherNone(keyValues, level), () => assertLowerNone(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertEmpty(keyValues: Iterable[KeyValue],
                  level: LevelRef) = {
    val asserts =
      Seq(
        () => assertGetNone(keyValues, level),
        () => assertHigherNone(keyValues, level),
        () => assertLowerNone(keyValues, level),
        () => assertEmptyHeadAndLast(level)
      )
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertGetFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                 level: Level) =
    keyValues foreach {
      keyValue =>
        try {
          val actual = level.getFromThisLevel(keyValue.key).assertGet
          actual.getOrFetchValue.safeGetBlocking() shouldBe keyValue.getOrFetchValue.safeGetBlocking()
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertEmptyHeadAndLast(level: LevelRef) =
    Seq(
      () => level.head.get.safeGetBlocking shouldBe empty,
      () => level.last.get.safeGetBlocking shouldBe empty,
    ).runThisRandomlyInParallel

  def assertReads(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {

    //read fullIndex
//    readAll(reader.copy()).assertGet shouldBe keyValues
//    //find each KeyValue using all Matchers
//    assertGet(keyValues, reader.copy())
    //    assertLower(keyValues, reader.copy())
    assertHigher(keyValues, reader.copy())
  }

  def assertGet(keyValues: Iterable[KeyValue],
                segment: Segment) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 1000 == 0)
        //          println("Get: " + intKey)
        segment.get(keyValue.key).get.safeGetBlocking().assertGet shouldBe keyValue
    }

  def dump(segments: Iterable[Segment]): Iterable[String] =
    Seq(s"Segments: ${segments.size}") ++ {
      segments map {
        segment =>
          val stringInfos =
            unzipGroups(segment.getAll().get) map {
              keyValue =>
                keyValue.toMemory match {
                  case response: Memory.SegmentResponse =>
                    response match {
                      case fixed: Memory.Fixed =>
                        fixed match {
                          case Memory.Put(key, value, deadline, time) =>
                            s"""PUT - ${key.readInt()} -> ${value.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

                          case Memory.Update(key, value, deadline, time) =>
                            s"""UPDATE - ${key.readInt()} -> ${value.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

                          case Memory.Function(key, function, time) =>
                            s"""FUNCTION - ${key.readInt()} -> ${functionStore.get(function)}, ${time.time.readLong()}"""

                          case PendingApply(key, applies) =>
                            //                        s"""
                            //                           |${key.readInt()} -> ${functionStore.find(function)}, ${time.time.readLong()}
                            //                        """.stripMargin
                            "PENDING-APPLY"

                          case Memory.Remove(key, deadline, time) =>
                            s"""REMOVE - ${key.readInt()} -> ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""
                        }
                      case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
                        s"""RANGE - ${fromKey.readInt()} -> ${toKey.readInt()}, $fromValue (${fromValue.map(Value.hasTimeLeft)}), $rangeValue (${Value.hasTimeLeft(rangeValue)})"""
                    }

                  case Memory.Group(minKey, maxKey, deadline, valueLength, _) =>
                    fail("should have ungrouped.")
                }
            }

          s"""
             |segment: ${segment.path}
             |${stringInfos.mkString("\n")}
             |""".stripMargin + "\n"
      }
    }

  @tailrec
  def dump(level: NextLevel): Unit =
    level.nextLevel match {
      case Some(nextLevel) =>
        val data =
          Seq(s"\nLevel: ${level.rootPath}\n") ++
            dump(level.segmentsInLevel())
        IOEffect.write(Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"), Slice(Slice.writeString(data.mkString("\n")))).get

        dump(nextLevel)

      case None =>
        val data =
          Seq(s"\nLevel: ${level.rootPath}\n") ++
            dump(level.segmentsInLevel())
        IOEffect.write(Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"), Slice(Slice.writeString(data.mkString("\n")))).get
    }

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        try
          level.get(keyValue.key).get.safeGetBlocking() match {
            case Some(got) =>
              got shouldBe keyValue

            case None =>
              unexpiredPuts(Slice(keyValue)) should have size 0
          }
        catch {
          case ex: Throwable =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" expired: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            fail(ex)
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        try
          level.get(keyValue.key).get.safeGetBlocking shouldBe empty
        catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelZero) =
    unzipGroups(keyValues).par foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe None
    }

  def assertGetNone(keys: Range,
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key)).assertGetOpt shouldBe empty
    }

  def assertGetNone(keys: List[Int],
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key)).get.safeGetBlocking shouldBe empty
    }

  def assertGetNoneButLast(keyValues: Iterable[KeyValue],
                           level: LevelRef) = {
    unzipGroups(keyValues).dropRight(1).par foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe empty
    }

    keyValues
      .lastOption
      .map(_.key)
      .flatMap(level.get(_).assertGetOpt.map(_.toMemory)) shouldBe keyValues.lastOption
  }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key).assertGetOpt shouldBe empty
    }

  /**
    * If all key-values are non put key-values then searching higher for each key-value
    * can result in a very long search time. Considering using shuffleTake which
    * randomly selects a batch to assert for None higher.
    */
  def assertHigherNone(keyValues: Iterable[KeyValue],
                       level: LevelRef,
                       shuffleTake: Option[Int] = None) = {
    val unzipedKeyValues = unzipGroups(keyValues)
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          //          println(keyValue.key.readInt())
          level.higher(keyValue.key).assertGetOpt shouldBe empty
          //          println
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertLowerNone(keyValues: Iterable[KeyValue],
                      level: LevelRef,
                      shuffleTake: Option[Int] = None) = {
    val unzipedKeyValues = unzipGroups(keyValues)
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          level.lower(keyValue.key).assertGetOpt shouldBe empty
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertLower(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {
    //
    //    @tailrec
    //    def assertLowers(index: Int) {
    //      //      println(s"assertLowers : ${index}")
    //      if (index > keyValues.size - 1) {
    //        //end
    //      } else if (index == 0) {
    //        keyValues(index) match {
    //          case range: KeyValue.WriteOnly.Range =>
    //            SegmentReader.lower(KeyMatcher.Lower(range.fromKey), None, reader.copy()).assertGetOpt shouldBe empty
    //            SegmentReader.lower(KeyMatcher.Lower(range.toKey), None, reader.copy()).assertGetOpt shouldBe range
    //
    //          case _ =>
    //            SegmentReader.lower(KeyMatcher.Lower(keyValues(index).key), None, reader.copy()).assertGetOpt shouldBe empty
    //        }
    //        assertLowers(index + 1)
    //      } else {
    //        val expectedLowerKeyValue = keyValues(index - 1)
    //        keyValues(index) match {
    //          case range: KeyValue.WriteOnly.Range =>
    //            SegmentReader.lower(KeyMatcher.Lower(range.fromKey), None, reader.copy()).assertGet shouldBe expectedLowerKeyValue
    //            SegmentReader.lower(KeyMatcher.Lower(range.toKey), None, reader.copy()).assertGet shouldBe range
    //
    //          case _ =>
    //            SegmentReader.lower(KeyMatcher.Lower(keyValues(index).key), None, reader.copy()).assertGet shouldBe expectedLowerKeyValue
    //        }
    //
    //        assertLowers(index + 1)
    //      }
    //    }
    //
    //    assertLowers(0)
    ???
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Unit = {
    val (footer, values, sortedIndex, hashIndex, binarySearchIndex, bloomFilter) = readBlocks(reader).get
    assertHigher(
      keyValues,
      getHigher =
        key =>
          SegmentSearcher.higher(
            key = key,
            start = None,
            end = None,
            binarySearch = binarySearchIndex,
            sortedIndex = sortedIndex,
            values = values
          )
    )
  }

  def assertLower(_keyValues: Slice[KeyValue],
                  segment: Segment) = {

    val keyValues = unzipGroups(_keyValues)

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        //        println(s"Lower: ${actualKeyValue.key.readInt()}")
        segment.lower(actualKeyValue.key).get.safeGetBlocking() shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 100 == 0)
        //          println(s"Lower: $intKey")
        try {
          val lower = segment.lower(keyValue.key).get.safeGetBlocking().assertGet
          lower shouldBe expectedLower
        } catch {
          case x: Exception =>
            x.printStackTrace()
            throw x
        }
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def unzipGroups[T <: KeyValue](keyValues: Iterable[T])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                         keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] =
    keyValues.flatMap {
      case keyValue: KeyValue.WriteOnly.Group =>
        unzipGroups(keyValue.keyValues)
      case keyValue: KeyValue.ReadOnly.Group =>
        unzipGroups(keyValue.segment.getAll().get.safeGetBlocking())
      case keyValue: KeyValue =>
        Slice(keyValue.toMemory)
    }.toMemory.toTransient

  def assertHigher(keyValues: Slice[KeyValue],
                   segment: Segment): Unit =
    assertHigher(unzipGroups(keyValues), getHigher = key => IO(segment.higher(key).get.safeGetBlocking()))

  /**
    * Asserts that all key-values are returned in order when fetching higher in sequence.
    */
  def assertHigher(_keyValues: Iterable[KeyValue],
                   getHigher: Slice[Byte] => IO[Option[KeyValue]]): Unit = {
    import KeyOrder.default._
    val keyValues = _keyValues.toMemory.toArray

    //assert higher if the currently's read key-value is the last key-value
    def assertLast(keyValue: KeyValue) =
      keyValue match {
        case range: KeyValue.ReadOnly.Range =>
          getHigher(range.fromKey).assertGet shouldBe range
          getHigher(range.toKey).assertGetOpt shouldBe empty

        case group: KeyValue.ReadOnly.Group =>
          if (group.minKey equiv group.maxKey.maxKey) {
            getHigher(group.minKey).assertGetOpt shouldBe empty
          } else {
            getHigher(group.minKey).assertGet shouldBe group
            getHigher(group.maxKey.maxKey).assertGetOpt shouldBe empty
          }

        case keyValue =>
          getHigher(keyValue.key).assertGetOpt shouldBe empty
      }

    //assert higher if the currently's read key-value is NOT the last key-value
    def assertNotLast(keyValue: KeyValue,
                      next: KeyValue,
                      nextNext: Option[KeyValue]) = {
      def shouldBeNextNext(higher: Option[KeyValue]) =
        if (nextNext.isEmpty) //if there is no 3rd key, higher should be empty
          higher shouldBe empty
        else
          higher.assertGet shouldBe nextNext.assertGet

      keyValue match {
        case range: KeyValue.ReadOnly.Range =>
          getHigher(range.fromKey).assertGet shouldBe range
          val toKeyHigher = getHigher(range.toKey).assertGetOpt
          //suppose this keyValue is Range (1 - 10), second is Put(10), third is Put(11), higher on Range's toKey(10) will return 11 and not 10.
          //but 10 will be return if the second key-value was a range key-value.
          //if the toKey is equal to expected higher's key, then the higher is the next 3rd key.
          next match {
            case next: KeyValue.ReadOnly.Range =>
              toKeyHigher.assertGet shouldBe next

            case _ =>
              //if the range's toKey is the same as next key, higher is next's next.
              //or if the next is group then
              if (next.key equiv range.toKey)
                next match {
                  case nextGroup: KeyValue.ReadOnly.Group if nextGroup.minKey != nextGroup.maxKey.maxKey =>
                    toKeyHigher.assertGet shouldBe nextGroup

                  case _ =>
                    shouldBeNextNext(toKeyHigher)
                }
              else
                toKeyHigher.assertGet shouldBe next
          }

        case group: KeyValue.ReadOnly.Group if group.minKey != group.maxKey.maxKey =>
          getHigher(group.minKey).assertGet shouldBe group
          getHigher(group.maxKey.maxKey).assertGet shouldBe next

        case _ =>
          IO(getHigher(keyValue.key).assertGet shouldBe next) recover {
            case _: TestFailedException =>
              unexpiredPuts(Slice(next)) should have size 0
          }
      }
    }

    keyValues.indices foreach {
      index =>
        if (index == keyValues.length - 1) { //last index
          assertLast(keyValues(index))
        } else {
          val next = keyValues(index + 1)
          val nextNext = IO(keyValues(index + 2)).toOption
          assertNotLast(keyValues(index), next, nextNext)
        }
    }
  }

  def expiredDeadline(): Deadline = {
    val deadline = 0.nanosecond.fromNow - 100.millisecond
    deadline.hasTimeLeft() shouldBe false
    deadline
  }

  def randomExpiredDeadlineOption(): Option[Deadline] =
    if (randomBoolean)
      None
    else
      Some(expiredDeadline())

  def readAll(group: Transient.Group): IO[Slice[KeyValue.ReadOnly]] = {
    val segment = SegmentBlock.write(Slice(group), 0, Seq.empty).get
    readAll(segment)
  }

  def readBlocks(group: Transient.Group): IO[(SegmentBlock.Footer, Option[BlockReader[Values]], BlockReader[SortedIndex], Option[BlockReader[HashIndex]], Option[BlockReader[BinarySearchIndex]], Option[BlockReader[BloomFilter]])] = {
    val segment = SegmentBlock.write(Slice(group), 0, Seq.empty).get
    readBlocks(segment)
  }

  def readAll(closedSegment: SegmentBlock.ClosedSegment): IO[Slice[KeyValue.ReadOnly]] =
    readAll(closedSegment.flattenSegmentBytes)

  def readBlocks(closedSegment: SegmentBlock.ClosedSegment): IO[(SegmentBlock.Footer, Option[BlockReader[Values]], BlockReader[SortedIndex], Option[BlockReader[HashIndex]], Option[BlockReader[BinarySearchIndex]], Option[BlockReader[BloomFilter]])] =
    readBlocks(closedSegment.flattenSegmentBytes)

  def readAll(bytes: Slice[Byte]): IO[Slice[KeyValue.ReadOnly]] =
    readAll(Reader(bytes))

  def readBlocks(bytes: Slice[Byte]): IO[(SegmentBlock.Footer, Option[BlockReader[Values]], BlockReader[SortedIndex], Option[BlockReader[HashIndex]], Option[BlockReader[BinarySearchIndex]], Option[BlockReader[BloomFilter]])] =
    readBlocks(Reader(bytes))

  def readAll(reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): IO[Slice[KeyValue.ReadOnly]] =
    for {
      segmentBlockReader <- SegmentBlock.read(SegmentBlock.Offset(0, reader.size.get.toInt), reader).map(_.createBlockReader(reader))
      footer <- SegmentBlock.readFooter(segmentBlockReader)
      valuesReader <- footer.valuesOffset.map(Values.read(_, segmentBlockReader).map(_.createBlockReader(segmentBlockReader)).map(Some(_))) getOrElse IO.none
      sortedIndex <- SortedIndex.read(footer.sortedIndexOffset, segmentBlockReader).map(_.createBlockReader(segmentBlockReader))
      all <- {
        SortedIndex
          .readAll(
            keyValueCount = footer.keyValueCount,
            sortedIndexReader = sortedIndex,
            valuesReader = valuesReader
          )
      }
    } yield all

  def readBlocks(reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) =
    for {
      segmentBlockReader <- SegmentBlock.read(SegmentBlock.Offset(0, reader.size.get.toInt), reader).map(_.createBlockReader(reader))
      footer <- SegmentBlock.readFooter(segmentBlockReader)
      valuesReader <- footer.valuesOffset.map(Values.read(_, segmentBlockReader).map(_.createBlockReader(segmentBlockReader)).map(Some(_))) getOrElse IO.none
      sortedIndex <- SortedIndex.read(footer.sortedIndexOffset, segmentBlockReader).map(_.createBlockReader(segmentBlockReader))
      hashIndex <- footer.hashIndexOffset.map(HashIndex.read(_, segmentBlockReader).map(_.createBlockReader(segmentBlockReader)).map(Some(_))) getOrElse IO.none
      binarySearchIndex <- footer.binarySearchIndexOffset.map(BinarySearchIndex.read(_, segmentBlockReader).map(_.createBlockReader(segmentBlockReader)).map(Some(_))) getOrElse IO.none
      bloomFilter <- footer.bloomFilterOffset.map(BloomFilter.read(_, segmentBlockReader).map(_.createBlockReader(segmentBlockReader)).map(Some(_))) getOrElse IO.none
    } yield (footer, valuesReader, sortedIndex, hashIndex, binarySearchIndex, bloomFilter)

  def printGroupHierarchy(keyValues: Slice[KeyValue.ReadOnly], spaces: Int)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                            keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Unit =
    keyValues foreachBreak {
      case group: Persistent.Group =>
        println(s"$spaces " + " " * spaces + group.getClass.getSimpleName)
        printGroupHierarchy(group.segment.getAll().assertGet, spaces + 1)
        false
      case group: Memory.Group =>
        println(s"$spaces " + " " * spaces + group.getClass.getSimpleName)
        printGroupHierarchy(group.segment.getAll().assertGet, spaces + 1)
        false
      case _ =>
        true
    }

  def printGroupHierarchy(segments: Slice[Segment]): Unit =
    segments foreach {
      segment =>
        println(s"Segment: ${segment.path}")
        printGroupHierarchy(segment.getAll().assertGet, 0)
    }

  def openGroups(keyValues: Slice[KeyValue.ReadOnly]): Slice[KeyValue.ReadOnly] =
    keyValues flatMap {
      case group: KeyValue.ReadOnly.Group =>
        openGroup(group)

      case keyValue =>
        Slice(keyValue)
    }

  def openGroup(group: KeyValue.ReadOnly.Group)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[KeyValue.ReadOnly] = {
    val allKeyValues = group.segment.getAll().assertGet
    allKeyValues flatMap {
      case group: KeyValue.ReadOnly.Group =>
        openGroup(group)

      case keyValue =>
        Slice(keyValue)
    }
  }

  def collapseMerge(newKeyValue: Memory.Fixed,
                    oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.ReadOnly.Fixed =
    newKeyValue match {
      case PendingApply(_, newApplies) =>
        //PendingApplies on PendingApplies are never merged. They are just stashed in sequence of their time.
        Memory.PendingApply(newKeyValue.key, oldApplies ++ newApplies)

      case _ =>
        var count = 0
        //reversing so that order is where newer are at the head.
        val reveredApplied = oldApplies.reverse.toList
        reveredApplied.foldLeft(newKeyValue: KeyValue.ReadOnly.Fixed) {
          case (newer, older) =>
            count += 1
            //merge as though applies were normal fixed key-values. The result should be the same.
            FixedMerger(newer, older.toMemory(newKeyValue.key)).assertGet match {
              case newPendingApply: ReadOnly.PendingApply =>
                val resultApplies = newPendingApply.getOrFetchApplies.get.safeGetBlocking().reverse.toList ++ reveredApplied.drop(count)
                val result =
                  if (resultApplies.size == 1)
                    resultApplies.head.toMemory(newKeyValue.key)
                  else
                    Memory.PendingApply(key = newKeyValue.key, resultApplies.reverse.toSlice)
                return result

              case other =>
                other
            }
        }
    }

  def assertNotSliced(keyValue: KeyValue.ReadOnly): Unit =
    IO(assertSliced(keyValue)).failed.assertGet

  def assertSliced(value: Value): Unit =
    value match {
      case Value.Remove(deadline, time) =>
        time.time.shouldBeSliced()

      case Value.Update(value, deadline, time) =>
        value.shouldBeSliced()
        time.time.shouldBeSliced()

      case Value.Function(function, time) =>
        function.shouldBeSliced()
        time.time.shouldBeSliced()

      case Value.PendingApply(applies) =>
        applies foreach assertSliced

      case Value.Put(value, deadline, time) =>
        value.shouldBeSliced()
        time.time.shouldBeSliced()
    }

  def assertSliced(value: Iterable[Value]): Unit =
    value foreach assertSliced

  def assertSliced(keyValue: KeyValue.ReadOnly): Unit =
    keyValue match {
      case memory: Memory =>
        memory match {
          case Memory.Put(key, value, deadline, time) =>
            key.shouldBeSliced()
            value.shouldBeSliced()
            time.time.shouldBeSliced()

          case Memory.Update(key, value, deadline, time) =>
            key.shouldBeSliced()
            value.shouldBeSliced()
            time.time.shouldBeSliced()

          case Memory.Function(key, function, time) =>
            key.shouldBeSliced()
            function.shouldBeSliced()
            time.time.shouldBeSliced()

          case PendingApply(key, applies) =>
            key.shouldBeSliced()
            assertSliced(applies)

          case Memory.Remove(key, deadline, time) =>
            key.shouldBeSliced()
            time.time.shouldBeSliced()

          case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
            fromKey.shouldBeSliced()
            toKey.shouldBeSliced()
            fromValue foreach assertSliced
            assertSliced(rangeValue)

          case Memory.Group(minKey, maxKey, deadline, valueLength, _) =>
            minKey.shouldBeSliced()
            maxKey.maxKey.shouldBeSliced()
          //todo assert decompressed length
          //            groupDecompressor.reader().assertGet.remaining.assertGet
        }
      case persistent: Persistent =>
        persistent match {
          case Persistent.Remove(_key, deadline, _time, indexOffset, nextIndexOffset, nextIndexSize, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()

          case Persistent.Put(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchValue.get.safeGetBlocking().shouldBeSliced()

          case Persistent.Update(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchValue.get.safeGetBlocking().shouldBeSliced()

          case Persistent.Function(_key, lazyFunctionReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyFunctionReader.getOrFetchFunction.get.safeGetBlocking().shouldBeSliced()

          case Persistent.PendingApply(_key, _time, deadline, lazyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchApplies.get.safeGetBlocking() foreach assertSliced

          case Persistent.Range(_fromKey, _toKey, lazyRangeValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _fromKey.shouldBeSliced()
            _toKey.shouldBeSliced()
            lazyRangeValueReader.fetchFromValue.assertGetOpt foreach assertSliced
            lazyRangeValueReader.fetchRangeValue foreach assertSliced

          case Persistent.Group(_minKey, _maxKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, deadline, _, _, _) =>
            _minKey.shouldBeSliced()
            _maxKey.maxKey.shouldBeSliced()
            valueReader.moveTo(valueOffset).read(valueLength).safeGetBlocking().get.shouldBeSliced()
        }
    }

  def countRangesManually(keyValues: Iterable[KeyValue.WriteOnly]): Int =
    keyValues.foldLeft(0) {
      case (count, keyValue) =>
        keyValue match {
          case fixed: WriteOnly.Fixed =>
            count
          case range: WriteOnly.Range =>
            count + 1
          case group: WriteOnly.Group =>
            count + countRangesManually(group.keyValues)
        }
    }

  implicit class BooleanImplicit(bool: Boolean) {
    def toInt =
      if (bool) 1 else 0
  }

  def assertGroup(group: Transient.Group)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                          limiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Persistent.Group = {
    val readKeyValues = readAll(group).get
    readKeyValues should have size 1
    val persistedGroup = readKeyValues.head.asInstanceOf[Persistent.Group]
    val groupKeyValues = persistedGroup.segment.getAll().get
    groupKeyValues should have size group.keyValues.size
    groupKeyValues shouldBe group.keyValues
    persistedGroup.segmentBlock.isCached shouldBe true
    persistedGroup
  }
}
