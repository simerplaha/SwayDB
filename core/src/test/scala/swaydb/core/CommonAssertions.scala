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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import bloomfilter.mutable.BloomFilter
import java.util.concurrent.ConcurrentSkipListMap
import org.scalatest.Assertion
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}
import swaydb.compression.CompressionInternal
import swaydb.core.TestData._
import swaydb.core.data.KeyValue.{ReadOnly, WriteOnly}
import swaydb.core.data.Memory.PendingApply
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.group.compression.GroupCompressor
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.level.zero.{LevelZero, LevelZeroSkipListMerger}
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryWriter, RangeValueSerializer, ValueSerializer}
import swaydb.core.merge._
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.{KeyMatcher, SegmentFooter, SegmentReader, SegmentWriter}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.CollectionUtil._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.StorageUnits._
import TryAssert._
import RunThis._
import swaydb.core.retry.Retry
import swaydb.core.util.TryUtil

object CommonAssertions {

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
          actual.segmentCache.getAll().assertGet shouldBe expected.segmentCache.getAll().assertGet
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
              Some(keyValue.getOrFetchFunction.assertGet)
            case keyValue: Memory.PendingApply =>
              val bytes = Slice.create[Byte](ValueSerializer.bytesRequired(keyValue.getOrFetchApplies.assertGet))
              ValueSerializer.write(keyValue.getOrFetchApplies.assertGet)(bytes)
              Some(bytes)
            case keyValue: Memory.Remove =>
              None
            case keyValue: Memory.Range =>
              val bytes = Slice.create[Byte](RangeValueSerializer.bytesRequired(keyValue.fromValue, keyValue.rangeValue))
              RangeValueSerializer.write(keyValue.fromValue, keyValue.rangeValue)(bytes)
              Some(bytes)

            case keyValue: Memory.Group =>
              Option(keyValue.groupDecompressor.reader().assertGet.readRemaining().assertGet)
          }
        case keyValue: Transient =>
          keyValue match {
            case keyValue: Transient.Put =>
              keyValue.value
            case keyValue: Transient.Update =>
              keyValue.value
            case keyValue: Transient.Function =>
              keyValue.value
            case keyValue: Transient.PendingApply =>
              keyValue.value
            case keyValue: Transient.Remove =>
              keyValue.value
            case keyValue: Transient.Range =>
              keyValue.value
            case keyValue: Transient.Group =>
              keyValue.value
          }
        case keyValue: Persistent =>
          keyValue match {
            case keyValue: Persistent.Put =>
              keyValue.getOrFetchValue.assertGetOpt
            case keyValue: Persistent.Update =>
              keyValue.getOrFetchValue.assertGetOpt
            case keyValue: Persistent.Function =>
              Some(keyValue.getOrFetchFunction.assertGet)
            case keyValue: Persistent.PendingApply =>
              keyValue.lazyValueReader.getOrFetchValue.assertGetOpt
            case _: Persistent.Remove =>
              None
            case keyValue: Persistent.Range =>
              keyValue.lazyRangeValueReader.getOrFetchValue.assertGetOpt
            case keyValue: Persistent.Group =>
              keyValue.lazyGroupValueReader.getOrFetchValue.assertGetOpt
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
                  eitherOne(
                    left = GroupGroupingStrategyInternal.Count(
                      count = randomIntMax(5) max 1,
                      indexCompression = TestData.randomCompression(),
                      valueCompression = TestData.randomCompression()
                    ),
                    right =
                      GroupGroupingStrategyInternal.Size(
                        size = randomIntMax(keyValuesCount max 1000).bytes * 2,
                        indexCompression = TestData.randomCompression(),
                        valueCompression = TestData.randomCompression()
                      )
                  )
                ),
              right = None
            ),
          indexCompression = TestData.randomCompression(),
          valueCompression = TestData.randomCompression()
        ),
      right =
        KeyValueGroupingStrategyInternal.Size(
          size = keyValuesCount.kb,
          groupCompression =
            eitherOne(
              left =
                Some(
                  eitherOne(
                    left = GroupGroupingStrategyInternal.Count(
                      count = randomIntMax(5) max 1,
                      indexCompression = TestData.randomCompression(),
                      valueCompression = TestData.randomCompression()
                    ),
                    right =
                      GroupGroupingStrategyInternal.Size(
                        size = randomIntMax(500).kb,
                        indexCompression = TestData.randomCompression(),
                        valueCompression = TestData.randomCompression()
                      )
                  )
                ),
              right = None
            ),
          indexCompression = TestData.randomCompression(),
          valueCompression = TestData.randomCompression()
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

  implicit class MergeKeyValues(keyValues: (Memory.Fixed, Memory.Fixed)) {
    def merge: ReadOnly.Fixed =
    //      KeyValueMerger(
    //        key = Some(keyValues._1.key),
    //        newKeyValue = keyValues._1.toValue().assertGet,
    //        oldKeyValue = keyValues._2.toValue().assertGet
    //      ).assertGet.toMemory(keyValues._1.key)
      ???

    def mergeFailed: Throwable =
    //      KeyValueMerger(
    //        key = Some(keyValues._1.key),
    //        newKeyValue = keyValues._1.toValue().assertGet,
    //        oldKeyValue = keyValues._2.toValue().assertGet
    //      ).failed.assertGet
      ???
  }

  implicit class MergeKeyValue(newKeyValue: Memory.Fixed) {
    def merge(oldKeyValue: Memory.Fixed): ReadOnly.Fixed =
    //      KeyValueMerger(
    //        key = Some(newKeyValue.key),
    //        newKeyValue = newKeyValue.toValue().assertGet,
    //        oldKeyValue = oldKeyValue.toValue().assertGet
    //      ).assertGet.toMemory(newKeyValue.key)
      ???

    def mergeNoKey(oldKeyValue: Memory.Fixed): ReadOnly.Fixed =
    //      KeyValueMerger(
    //        key = None,
    //        newKeyValue = newKeyValue.toValue().assertGet,
    //        oldKeyValue = oldKeyValue.toValue().assertGet
    //      ).assertGet.toMemory(newKeyValue.key)
      ???

    def mergeFailed(oldKeyValue: Memory.Fixed): Throwable =
    //      KeyValueMerger(
    //        key = Some(newKeyValue.key),
    //        newKeyValue = newKeyValue.toValue().assertGet,
    //        oldKeyValue = oldKeyValue.toValue().assertGet
    //      ).failed.assertGet
      ???
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
    val result = SegmentMerger.merge(newKeyValues, oldKeyValues, 10.mb, isLastLevel = isLastLevel, false, TestData.falsePositiveRate, compressDuplicateValues = true).assertGet
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
    assertMerge(newKeyValues, oldKeyValues, expected.toTransient, isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect.toTransient, isLastLevel = true)
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
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected.toTransient, isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect.toTransient, isLastLevel = true)
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
    def shouldHaveSameInOrderedIds(expected: Iterable[Segment]): Unit = {
      actual.map(_.path) shouldBe expected.map(_.path)
    }

    def shouldHaveSameIds(expected: Iterable[Segment]): Unit = {
      actual.map(_.path) should contain allElementsOf expected.map(_.path)
    }

    def shouldHaveSameKeyValuesAs(expected: Iterable[Segment]): Unit = {
      Segment.getAllKeyValues(TestData.falsePositiveRate, actual).assertGet shouldBe Segment.getAllKeyValues(TestData.falsePositiveRate, expected).assertGet
    }
  }

  implicit class SliceByteImplicits(actual: Slice[Byte]) {
    def shouldHaveSameKey(expected: KeyValue): Unit = {
      actual shouldBe expected.key
    }

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
    def shouldBe(expected: Option[Stats], ignoreValueOffset: Boolean = false) = {
      if (actual.isDefined && expected.isDefined)
        actual.assertGet shouldBe(expected.assertGet, ignoreValueOffset)
    }
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
      //      expected match {
      //        case expectedRange: KeyValue.WriteOnly.Range =>
      //          actual should be(a[Persistent.Range])
      //          val actualRange = actual.asInstanceOf[Persistent.Range]
      //          actualRange.fromKey shouldBe expectedRange.fromKey
      //          actualRange.toKey shouldBe expectedRange.toKey
      //          actualRange.fetchFromAndRangeValue.assertGet shouldBe expectedRange.fetchFromAndRangeValue.assertGet
      //          actualRange.fetchFromValue.assertGetOpt shouldBe expectedRange.fetchFromValue.assertGetOpt
      //          actualRange.fetchRangeValue.assertGet shouldBe expectedRange.fetchRangeValue.assertGet
      //
      //        case expectedGroup: KeyValue.WriteOnly.Group =>
      //          actual should be(a[Persistent.Group])
      //          actual.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe expectedGroup.keyValues
      //
      //        case expected: KeyValue.WriteOnly.Fixed =>
      //          actual.key shouldBe expected.key
      //          actual.getOrFetchValue shouldBe expected.getOrFetchValue
      //          actual.indexEntryDeadline shouldBe expected.deadline
      //      }

    }
  }

  implicit class PersistentReadOnlyImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) =
      actual.toMemory shouldBe expected.toMemory
  }

  implicit class StatsImplicits(actual: Stats) {

    def shouldBe(expected: Stats, ignoreValueOffset: Boolean = false): Assertion = {
      actual.segmentSize shouldBe expected.segmentSize
      actual.valueLength shouldBe expected.valueLength
      if (!ignoreValueOffset && actual.valueLength != 0) {
        //        actual.valueOffset shouldBe expected.valueOffset
        //        actual.toValueOffset shouldBe expected.toValueOffset
        ???
      }
      actual.segmentSizeWithoutFooter shouldBe expected.segmentSizeWithoutFooter
      actual.segmentValuesSize shouldBe expected.segmentValuesSize
      actual.thisKeyValuesIndexSizeWithoutFooter shouldBe expected.thisKeyValuesIndexSizeWithoutFooter
      actual.thisKeyValuesSegmentSizeWithoutFooter shouldBe expected.thisKeyValuesSegmentSizeWithoutFooter
    }
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
    assertHigher(keyValues, getHigher = key => level.higher(key))
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
        val expectedLowerKeyValue = keyValues(index - 1)
        val lower = level.lower(keyValues(index).key).assertGet
        lower.key shouldBe expectedLowerKeyValue.key
        lower.getOrFetchValue.assertGetOpt shouldBe expectedLowerKeyValue.getOrFetchValue
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertGet(keyValues: Slice[KeyValue.WriteOnly],
                reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) =
    keyValues foreach {
      keyValue =>
        SegmentReader.find(KeyMatcher.Get(keyValue.key), None, reader.copy()).assertGet shouldBe keyValue
    }

  def assertBloom(keyValues: Slice[KeyValue],
                  bloom: BloomFilter[Slice[Byte]]) = {
    keyValues foreach {
      keyValue =>
        bloom.mightContain(keyValue.key) shouldBe true
    }

    assertBloomNotContains(bloom)
  }

  def assertBloomNotContains(bloom: BloomFilter[Slice[Byte]]) =
    runThis(10.times) {
      Retry("bloom false test", (_, _) => TryUtil.successUnit, 10) {
        Try(bloom.mightContain(randomBytesSlice(randomIntMax(1000) min 100)) shouldBe false)
      }
    }

  def assertReads(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).foreach(_ ())
  }

  def assertReads(keyValues: Iterable[KeyValue],
                  level: LevelRef) = {
    val asserts = Seq(() => assertGet(keyValues, level), () => assertHigher(keyValues, level), () => assertLower(keyValues, level))
    Random.shuffle(asserts).foreach(_ ())
  }

  def assertGetFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                 level: Level) =
    keyValues foreach {
      keyValue =>
        val actual = level.getFromThisLevel(keyValue.key).assertGet
        actual.getOrFetchValue shouldBe keyValue.getOrFetchValue
    }

  def assertReads(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {

    val footer = SegmentReader.readFooter(reader.copy()).assertGet
    //read fullIndex
    SegmentReader.readAll(footer, reader.copy()).assertGet shouldBe keyValues
    //find each KeyValue using all Matchers
    assertGet(keyValues, reader.copy())
    assertLower(keyValues, reader.copy())
    assertHigher(keyValues, reader.copy())
  }

  def assertGet(keyValues: Iterable[KeyValue],
                segment: Segment) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        segment.get(keyValue.key).assertGet shouldBe keyValue
    }

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        try
          level.get(keyValue.key).assertGet shouldBe keyValue
        catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" expired: ${keyValue.toMemory.indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        try
          level.get(keyValue.key).assertGetOpt shouldBe empty
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
        Retry("assertGetNone", Retry.levelReadRetryUntil, 1000) {
          Try(level.get(Slice.writeInt(key)).assertGetOpt shouldBe empty) recoverWith {
            case exception: Exception =>
              Failure(exception.getCause)
          }
        }
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
    unzipGroups(keyValues).par foreach {
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
          //          println(keyValue.key.readInt())
          level.lower(keyValue.key).assertGetOpt shouldBe empty
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

  def assertLower(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {

    @tailrec
    def assertLowers(index: Int) {
      //      println(s"assertLowers : ${index}")
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        keyValues(index) match {
          case range: KeyValue.WriteOnly.Range =>
            SegmentReader.find(KeyMatcher.Lower(range.fromKey), None, reader.copy()).assertGetOpt shouldBe empty
            SegmentReader.find(KeyMatcher.Lower(range.toKey), None, reader.copy()).assertGetOpt shouldBe range

          case _ =>
            SegmentReader.find(KeyMatcher.Lower(keyValues(index).key), None, reader.copy()).assertGetOpt shouldBe empty
        }
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        keyValues(index) match {
          case range: KeyValue.WriteOnly.Range =>
            SegmentReader.find(KeyMatcher.Lower(range.fromKey), None, reader.copy()).assertGet shouldBe expectedLowerKeyValue
            SegmentReader.find(KeyMatcher.Lower(range.toKey), None, reader.copy()).assertGet shouldBe range

          case _ =>
            SegmentReader.find(KeyMatcher.Lower(keyValues(index).key), None, reader.copy()).assertGet shouldBe expectedLowerKeyValue
        }

        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Unit =
    assertHigher(
      keyValues,
      getHigher =
        key =>
          SegmentReader.find(KeyMatcher.Higher(key), None, reader.copy())
    )

  def assertLower(_keyValues: Slice[KeyValue],
                  segment: Segment) = {

    val keyValues = unzipGroups(_keyValues)

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        segment.lower(actualKeyValue.key).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        try {
          val lower = segment.lower(keyValue.key).assertGet
          lower shouldBe expectedLower
        } catch {
          case x: Exception =>
            x.printStackTrace()
            val lower = segment.lower(keyValue.key).assertGet
            throw x
        }
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def unzipGroups[T](keyValues: Iterable[T])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                             keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[KeyValue] =
    keyValues.flatMap {
      case keyValue: KeyValue.WriteOnly.Group =>
        unzipGroups(keyValue.keyValues)
      case keyValue: KeyValue.ReadOnly.Group =>
        unzipGroups(keyValue.segmentCache.getAll().assertGet)
      case keyValue: KeyValue =>
        Slice(keyValue)
    }.toMemory.toTransient

  def assertHigher(keyValues: Slice[KeyValue],
                   segment: Segment): Unit =
    assertHigher(unzipGroups(keyValues), getHigher = key => segment.higher(key))

  /**
    * Asserts that all key-values are returned in order when fetching higher in sequence.
    */
  def assertHigher(_keyValues: Iterable[KeyValue],
                   getHigher: Slice[Byte] => Try[Option[KeyValue]]): Unit = {
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
          getHigher(keyValue.key).assertGet shouldBe next
      }
    }

    keyValues.indices foreach {
      index =>
        if (index == keyValues.length - 1) { //last index
          assertLast(keyValues(index))
        } else {
          val next = keyValues(index + 1)
          val nextNext = Try(keyValues(index + 2)).toOption
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

  def assertGroup(group: Transient.Group,
                  expectedIndexCompressionUsed: CompressionInternal,
                  expectedValueCompressionUsed: Option[CompressionInternal])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {
    val groupKeyValues = group.keyValues
    //check if there are values exists in the group's key-values. Use this flag to read values bytes.
    val hasNoValues = groupKeyValues.forall(_.value.isEmpty)
    //if no values exist, no value compression should be used.
    if (hasNoValues) expectedValueCompressionUsed shouldBe empty

    group.minKey shouldBe (groupKeyValues.head.key: Slice[Byte])
    group.maxKey shouldBe groupKeyValues.maxKey()

    //read and assert keys info from the key bytes write
    val groupSegmentReader = Reader(group.compressedKeyValues) //create a reader from the compressed group key-values.
    //read the header bytes.

    //assert key info
    if (hasNoValues)
      groupSegmentReader.readIntUnsigned().assertGet should be >= 5.bytes
    else
      groupSegmentReader.readIntUnsigned().assertGet should be >= 8.bytes

    groupSegmentReader.readIntUnsigned().assertGet shouldBe GroupCompressor.formatId
    groupSegmentReader.readBoolean().assertGet shouldBe group.keyValues.last.stats.hasRange
    groupSegmentReader.readBoolean().assertGet shouldBe group.keyValues.last.stats.hasPut
    groupSegmentReader.readIntUnsigned().assertGet shouldBe expectedIndexCompressionUsed.decompressor.id //key decompression id
    groupSegmentReader.readIntUnsigned().assertGet shouldBe groupKeyValues.size //key-value count
    groupSegmentReader.readIntUnsigned().assertGet shouldBe groupKeyValues.last.stats.bloomFilterItemsCount //bloomFilterItemsCount count
    val keysDecompressLength = groupSegmentReader.readIntUnsigned().assertGet //keys length
    val keysCompressLength = groupSegmentReader.readIntUnsigned().assertGet //keys length

    //assert value info only if value bytes exists.
    val (valuesDecompressedBytes, valuesDecompressLength) =
      if (!hasNoValues) {
        groupSegmentReader.readIntUnsigned().assertGet shouldBe expectedValueCompressionUsed.assertGet.decompressor.id //value compression id
        val valuesDecompressLength = groupSegmentReader.readIntUnsigned().assertGet //values length
        val valuesCompressedLength = groupSegmentReader.readIntUnsigned().assertGet //values compressed length
        val valueCompressedBytes = groupSegmentReader.read(valuesCompressedLength).assertGet
        (expectedValueCompressionUsed.assertGet.decompressor.decompress(valueCompressedBytes, valuesDecompressLength).assertGet, valuesDecompressLength)
      } else {
        (Slice.empty, 0)
      }

    val keyCompressedBytes = groupSegmentReader.read(keysCompressLength).assertGet
    val keysDecompressedBytes = expectedIndexCompressionUsed.decompressor.decompress(keyCompressedBytes, keysDecompressLength).assertGet

    //merge both values and keys with values at the top and read key-values like it was a Segment.
    val keyValuesDecompressedBytes = valuesDecompressedBytes append keysDecompressedBytes

    val tempFooter = SegmentFooter(
      crc = 0,
      startIndexOffset = valuesDecompressLength,
      endIndexOffset = keyValuesDecompressedBytes.size - 1,
      keyValueCount = groupKeyValues.size,
      hasRange = false,
      hasPut = false,
      bloomFilterItemsCount = groupKeyValues.last.stats.bloomFilterItemsCount,
      bloomFilter = None
    )

    //read just the group bytes.
    val keyValues = SegmentReader.readAll(footer = tempFooter, reader = Reader(keyValuesDecompressedBytes)).assertGet

    keyValues should have size groupKeyValues.size
    keyValues shouldBe groupKeyValues

    //now write the Group to a full Segment and read it as a normal Segment.
    val (segmentBytes, nearestDeadline) = SegmentWriter.write(Seq(group.updateStats(TestData.falsePositiveRate, None)), TestData.falsePositiveRate).assertGet
    nearestDeadline shouldBe Segment.getNearestDeadline(groupKeyValues).assertGetOpt
    val segmentReader = Reader(segmentBytes)
    val footer = SegmentReader.readFooter(segmentReader).assertGet
    SegmentReader.readAll(footer, segmentReader.copy()).assertGet shouldBe Seq(group)
  }

  def readAll(bytes: Slice[Byte]): Try[Slice[KeyValue.ReadOnly]] =
    readAll(Reader(bytes))

  def readAll(reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Try[Slice[KeyValue.ReadOnly]] =
    SegmentReader.readFooter(reader) flatMap {
      footer =>
        SegmentReader.readAll(footer, reader.copy())
    }

  def printGroupHierarchy(keyValues: Slice[KeyValue.ReadOnly], spaces: Int)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                            keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Unit =
    keyValues.iterator foreachBreak {
      case group: Persistent.Group =>
        println(s"$spaces " + " " * spaces + group.getClass.getSimpleName)
        printGroupHierarchy(group.segmentCache.getAll().assertGet, spaces + 1)
        false
      case group: Memory.Group =>
        println(s"$spaces " + " " * spaces + group.getClass.getSimpleName)
        printGroupHierarchy(group.segmentCache.getAll().assertGet, spaces + 1)
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
    val allKeyValues = group.segmentCache.getAll().assertGet
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
                val resultApplies = newPendingApply.getOrFetchApplies.assertGet.reverse.toList ++ reveredApplied.drop(count)
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
    Try(assertSliced(keyValue)).failed.assertGet

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

          case Memory.Group(minKey, maxKey, deadline, groupDecompressor, valueLength) =>
            minKey.shouldBeSliced()
            maxKey.maxKey.shouldBeSliced()
          //todo assert decompressed length
          //            groupDecompressor.reader().assertGet.remaining.assertGet
        }
      case persistent: Persistent =>
        persistent match {
          case Persistent.Remove(_key, deadline, _time, indexOffset, nextIndexOffset, nextIndexSize) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()

          case Persistent.Put(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchValue.assertGetOpt.shouldBeSliced()

          case Persistent.Update(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchValue.assertGetOpt.shouldBeSliced()

          case Persistent.Function(_key, lazyFunctionReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyFunctionReader.getOrFetchFunction.assertGet.shouldBeSliced()

          case Persistent.PendingApply(_key, _time, deadline, lazyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            lazyValueReader.getOrFetchApplies.assertGet foreach assertSliced

          case Persistent.Range(_fromKey, _toKey, lazyRangeValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
            _fromKey.shouldBeSliced()
            _toKey.shouldBeSliced()
            lazyRangeValueReader.fetchFromValue.assertGetOpt foreach assertSliced
            lazyRangeValueReader.fetchRangeValue foreach assertSliced

          case Persistent.Group(_minKey, _maxKey, groupDecompressor, lazyGroupValueReader, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, deadline) =>
            _minKey.shouldBeSliced()
            _maxKey.maxKey.shouldBeSliced()
            lazyGroupValueReader.getOrFetchValue.assertGet.shouldBeSliced()

        }
    }

}
