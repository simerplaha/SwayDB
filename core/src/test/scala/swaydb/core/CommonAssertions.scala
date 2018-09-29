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

package swaydb.core

import java.util.concurrent.ConcurrentSkipListMap

import bloomfilter.mutable.BloomFilter
import org.scalatest.Assertion
import swaydb.compression.CompressionInternal
import swaydb.core.TestLimitQueues._
import swaydb.core.data.KeyValue.{ReadOnly, WriteOnly}
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.group.compression.GroupCompressor
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.level.zero.{LevelZero, LevelZeroRef, LevelZeroSkipListMerge}
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.MapEntryWriter
import swaydb.core.queue.{KeyValueLimiter, SegmentOpenLimiter}
import swaydb.core.segment.Segment
import swaydb.core.segment.format.one._
import swaydb.core.segment.merge.{KeyValueMerger, SegmentMerger}
import swaydb.core.util.CollectionUtil._
import swaydb.data.slice.{Reader, Slice, Slicer}
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Random, Try}

trait CommonAssertions extends TryAssert with FutureBase with TestData {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  implicit val keyValueLimiter = TestLimitQueues.keyValueLimiter

  def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

  def randomCompressionTypeOption(keyValuesCount: Int): Option[KeyValueGroupingStrategyInternal] =
    eitherOne(
      left = None,
      right = Some(randomCompressionType(keyValuesCount))
    )

  def randomCompressionType(keyValuesCount: Int): KeyValueGroupingStrategyInternal =
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
                      indexCompression = randomCompression(),
                      valueCompression = randomCompression()
                    ),
                    right =
                      GroupGroupingStrategyInternal.Size(
                        size = randomIntMax(keyValuesCount max 1000).bytes * 2,
                        indexCompression = randomCompression(),
                        valueCompression = randomCompression()
                      )
                  )
                ),
              right = None
            ),
          indexCompression = randomCompression(),
          valueCompression = randomCompression()
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
                      indexCompression = randomCompression(),
                      valueCompression = randomCompression()
                    ),
                    right =
                      GroupGroupingStrategyInternal.Size(
                        size = randomIntMax(500).kb,
                        indexCompression = randomCompression(),
                        valueCompression = randomCompression()
                      )
                  )
                ),
              right = None
            ),
          indexCompression = randomCompression(),
          valueCompression = randomCompression()
        )
    )

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

  def anyOrder[T](execution1: => T, execution2: => T): Unit =
    if (Random.nextBoolean()) {
      execution1
      execution2
    } else {
      execution2
      execution1
    }

  def runThis(times: Int)(f: => Unit): Unit =
    (1 to times) foreach {
      i =>
        //        println(s"Iteration number: $i")
        f
    }

  implicit class IsExpectedInLastLevel(fromValue: FromValue) {
    def toExpectedLastLevelKeyValue(key: Slice[Byte]): Option[Memory.Fixed] =
      fromValue match {
        case _: Value.Remove =>
          None
        case Value.Put(value, deadline) =>
          if (deadline.forall(_.hasTimeLeft()))
            Some(Memory.Put(key, value, deadline))
          else
            None
        case _: Value.Update =>
          None
        case _: Value.UpdateFunction =>
          None

      }
  }

  implicit class IsKeyValueExpectedInLastLevel(keyValue: Memory.Fixed) {
    def isExpectedInLastLevel: Boolean =
      keyValue match {
        case Memory.Put(key, value, deadline) =>
          if (deadline.forall(_.hasTimeLeft()))
            true
          else
            false
        case _: Memory.Update | _: Memory.UpdateFunction | _: Memory.Remove =>
          false
      }
  }

  implicit class ApplyValue(keyValues: (Memory.Fixed, Memory.Fixed)) {
    def merge: ReadOnly.Fixed =
      KeyValueMerger.applyValue(keyValues._1, keyValues._2, 10.seconds).assertGet
  }

  implicit class KeyValueWriteOnlyImplicits(keyValues: Iterable[KeyValue.WriteOnly]) {

    def toMemory: Slice[Memory] = {
      val slice = Slice.create[Memory](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toMemory
      }
      slice
    }

    def toMemoryResponse: Slice[Memory.Response] = {
      val slice = Slice.create[Memory.Response](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toMemoryResponse
      }
      slice
    }
  }

  implicit class ToSlice[T: ClassTag](items: Iterable[T]) {
    def toSlice: Slice[T] = {
      val slice = Slice.create[T](items.size)
      items foreach {
        item =>
          slice add item
      }
      slice
    }
  }

  implicit class WriteOnlyToMemory(keyValue: KeyValue.WriteOnly) {
    def toMemoryResponse: Memory.Response =
      keyValue match {
        case fixed: KeyValue.WriteOnly.Fixed =>
          fixed match {
            case Transient.Remove(key, deadline, previous, falsePositiveRate) =>
              Memory.Remove(key, deadline)

            case Transient.Update(key, value, deadline, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.Update(key, value, deadline)

            case Transient.UpdateFunction(key, value, deadline, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.UpdateFunction(key, value, deadline)

            case Transient.Put(key, value, deadline, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.Put(key, value, deadline)
          }

        case range: KeyValue.WriteOnly.Range =>
          range match {
            case Transient.Range(fromKey, toKey, fullKey, fromValue, rangeValue, value, previous, falsePositiveRate) =>
              Memory.Range(fromKey, toKey, fromValue, rangeValue)
          }
      }

    def toMemoryGroup: Memory.Group =
      keyValue match {
        case group: KeyValue.WriteOnly.Group =>
          group match {
            case Transient.Group(fromKey, toKey, fullKey, compressedKeyValues, deadline, keyValues, previous, falsePositiveRate) =>
              Memory.Group(
                minKey = fromKey,
                maxKey = toKey,
                compressedKeyValues = compressedKeyValues.unslice(),
                deadline = deadline,
                groupStartOffset = 0
              )
          }
      }

    def toMemory: Memory = {
      keyValue match {
        case group: KeyValue.WriteOnly.Group =>
          group match {
            case Transient.Group(fromKey, toKey, fullKey, compressedKeyValues, deadline, keyValues, previous, falsePositiveRate) =>
              Memory.Group(
                minKey = fromKey,
                maxKey = toKey,
                compressedKeyValues = compressedKeyValues.unslice(),
                deadline = deadline,
                groupStartOffset = 0
              )
          }

        case _ =>
          toMemoryResponse

      }
    }
  }

  implicit class WriteOnlysToMemory(keyValues: Iterable[KeyValue]) {
    def toMemory: Iterable[Memory] = {
      keyValues map {
        case readOnly: ReadOnly =>
          readOnly.toMemory
        case writeOnly: WriteOnly =>
          writeOnly.toMemory

      }
    }
  }

  implicit class ReadOnlyToMemory(keyValues: Iterable[KeyValue.ReadOnly]) {
    def toTransient: Slice[Transient] = {
      val slice = Slice.create[Transient](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toTransient.updateStats(0.1, slice.lastOption).asInstanceOf[Transient]
      }
      slice
    }

    def toMemory: Slice[Memory] = {
      val slice = Slice.create[Memory](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toMemory
      }
      slice
    }
  }

  implicit class ReadOnlyKeyValueToMemory(keyValue: KeyValue.ReadOnly) {

    import swaydb.core.map.serializer.RangeValueSerializers._

    def toTransient: Transient = {
      keyValue match {
        case memory: Memory =>
          memory match {
            case fixed: Memory.Fixed =>
              fixed match {
                case Memory.Put(key, value, deadline) =>
                  Transient.Put(key, value, 0.1, None, deadline, compressDuplicateValues = true)

                case Memory.Update(key, value, deadline) =>
                  Transient.Update(key, value, 0.1, None, deadline, compressDuplicateValues = true)

                case Memory.UpdateFunction(key, value, deadline) =>
                  Transient.UpdateFunction(key, value, 0.1, None, deadline, compressDuplicateValues = true)

                case Memory.Remove(key, deadline) =>
                  Transient.Remove(key, 0.1, None, deadline)
              }
            case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
              Transient.Range[Value.FromValue, Value.RangeValue](fromKey, toKey, fromValue, rangeValue, 0.1, None)

            case group @ Memory.Group(fromKey, toKey, nearestDeadline, groupDecompressor, _) =>
              val keyValues: Iterable[KeyValue.WriteOnly] = group.segmentCache.getAll().assertGet.toTransient
              Transient.Group(
                keyValues = keyValues,
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                falsePositiveRate = 0,
                previous = None
              ).assertGet
          }

        case persistent: Persistent =>
          persistent match {
            case persistent: Persistent.Fixed =>
              persistent match {
                case put @ Persistent.Put(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.Put(key, put.getOrFetchValue.assertGetOpt, 0.1, None, deadline, compressDuplicateValues = true)

                case put @ Persistent.Update(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.Update(key, put.getOrFetchValue.assertGetOpt, 0.1, None, deadline, compressDuplicateValues = true)

                case put @ Persistent.UpdateFunction(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.UpdateFunction(key, put.getOrFetchValue.assertGet, 0.1, None, deadline, compressDuplicateValues = true)

                case Persistent.Remove(_key, deadline, indexOffset, nextIndexOffset, nextIndexSize) =>
                  Transient.Remove(_key, 0.1, None, deadline)
              }

            case range @ Persistent.Range(_fromKey, _toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValue.assertGet
              Transient.Range(_fromKey, _toKey, fromValue, rangeValue, 0.1, None)

            case group @ Persistent.Group(_fromKey, _toKey, groupDecompressor, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, deadline) =>
              val allKeyValues = group.segmentCache.getAll().assertGet.toTransient
              Transient.Group(
                keyValues = allKeyValues,
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                previous = None,
                falsePositiveRate = 0.1
              ).assertGet
          }
      }
    }

    def toMemoryGroup =
      keyValue match {
        case Persistent.Group(minKey, maxKey, groupDecompressor, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, deadline) =>
          Memory.Group(
            minKey = minKey,
            maxKey = maxKey,
            deadline = deadline,
            groupDecompressor = groupDecompressor,
            valueLength = valueLength
          )
      }

    def toMemory: Memory = {
      keyValue match {
        case memory: Memory =>
          memory

        case persistent: Persistent.Response =>
          persistent.toMemoryResponse

        case persistent: Persistent.Group =>
          persistent.toMemoryGroup
      }
    }

    def toMemoryResponse: Memory.Response = {
      keyValue match {
        case memory: Memory.Response =>
          memory

        case persistent: Persistent =>
          persistent match {
            case persistent: Persistent.Fixed =>
              persistent match {
                case put @ Persistent.Put(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.Put(key, put.getOrFetchValue.assertGetOpt, deadline)

                case put @ Persistent.Update(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.Update(key, put.getOrFetchValue.assertGetOpt, deadline)

                case put @ Persistent.UpdateFunction(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.UpdateFunction(key, put.getOrFetchValue.assertGet, deadline)

                case Persistent.Remove(_key, deadline, indexOffset, nextIndexOffset, nextIndexSize) =>
                  Memory.Remove(_key, deadline)
              }

            case range @ Persistent.Range(_fromKey, _toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValue.assertGet
              Memory.Range(_fromKey, _toKey, fromValue, rangeValue)
          }
      }
    }
  }

  implicit class PrintSkipList(skipList: ConcurrentSkipListMap[Slice[Byte], Memory]) {

    import swaydb.serializers.Default._
    import swaydb.serializers._

    //stringify the skipList so that it's readable
    def asString(value: Value): String =
      value match {
        case Value.Remove(deadline) =>
          s"Remove(deadline = $deadline)"
        case Value.Put(value, deadline) =>
          s"Put(${value.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
        case Value.Update(value, deadline) =>
          s"Update(${value.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
        case Value.UpdateFunction(value, deadline) =>
          s"UpdateFunction(${value.read[String]}, deadline = $deadline)"
      }
  }

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly.Response],
                          oldKeyValues: Iterable[KeyValue.ReadOnly.Response],
                          expected: KeyValue.WriteOnly,
                          hasTimeLeftAtLeast: FiniteDuration): ConcurrentSkipListMap[Slice[Byte], Memory.Response] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected), hasTimeLeftAtLeast)

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly.Response],
                          oldKeyValues: Iterable[KeyValue.ReadOnly.Response],
                          expected: Iterable[KeyValue],
                          hasTimeLeftAtLeast: FiniteDuration): ConcurrentSkipListMap[Slice[Byte], Memory.Response] = {
    val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
    (oldKeyValues ++ newKeyValues).map(_.toMemoryResponse) foreach (memory => LevelZeroSkipListMerge(hasTimeLeftAtLeast).insert(memory.key, memory, skipList))
    skipList.size() shouldBe expected.size
    skipList.asScala.toList shouldBe expected.map(keyValue => (keyValue.key, keyValue.toMemory))
    skipList
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.Response,
                  oldKeyValue: KeyValue.ReadOnly.Response,
                  expected: Slice[KeyValue.WriteOnly],
                  hasTimeLeftAtLeast: FiniteDuration,
                  isLastLevel: Boolean = false)(implicit ordering: Ordering[Slice[Byte]],
                                                groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, hasTimeLeftAtLeast, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.Response],
                  oldKeyValues: Slice[KeyValue.ReadOnly.Response],
                  expected: Slice[KeyValue.WriteOnly],
                  hasTimeLeftAtLeast: FiniteDuration,
                  isLastLevel: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                        groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] = {
    val result = SegmentMerger.merge(newKeyValues, oldKeyValues, 10.mb, isLastLevel = isLastLevel, false, 0.1, hasTimeLeftAtLeast = hasTimeLeftAtLeast, compressDuplicateValues = true).assertGet
    if (expected.size == 0) {
      result shouldBe empty
    } else {
      result should have size 1
      result.head should have size expected.size
      result.head.toList should contain inOrderElementsOf expected
    }
    result
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.Response,
                  oldKeyValue: KeyValue.ReadOnly.Response,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: KeyValue.ReadOnly,
                  hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]],
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), Slice(lastLevelExpect), hasTimeLeftAtLeast)

  def assertMerge(newKeyValue: KeyValue.ReadOnly.Response,
                  oldKeyValue: KeyValue.ReadOnly.Response,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: Option[KeyValue.ReadOnly],
                  hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]],
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.map(Slice(_)).getOrElse(Slice.empty), hasTimeLeftAtLeast)
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected), hasTimeLeftAtLeast)
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.Response],
                  oldKeyValues: Slice[KeyValue.ReadOnly.Response],
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly],
                  hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]],
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValues, oldKeyValues, expected.toTransient, hasTimeLeftAtLeast, isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect.toTransient, hasTimeLeftAtLeast, isLastLevel = true)
    //println("*** Skip list assert ***")
    assertSkipListMerge(newKeyValues, oldKeyValues, expected, hasTimeLeftAtLeast)
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly.Response,
                  oldKeyValue: KeyValue.ReadOnly.Response,
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly],
                  hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]],
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected.toTransient, hasTimeLeftAtLeast, isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect.toTransient, hasTimeLeftAtLeast, isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly.Response],
                  oldKeyValues: Slice[KeyValue.ReadOnly.Response],
                  expected: KeyValue.WriteOnly,
                  hasTimeLeftAtLeast: FiniteDuration,
                  isLastLevel: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                        groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Iterable[Iterable[KeyValue.WriteOnly]] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), hasTimeLeftAtLeast, isLastLevel)

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

    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Response]]) =
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.Response]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory.Response](keyValue.key, keyValue.toMemoryResponse)
          mapEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class MemoryImplicits(actual: Iterable[Memory.Response]) {
    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Response]]) =
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.Response]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory.Response](keyValue.key, keyValue)
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
      Segment.getAllKeyValues(0.1, actual).assertGet shouldBe Segment.getAllKeyValues(0.1, expected).assertGet
    }
  }

  implicit class SliceByteImplicits(actual: Slice[Byte]) {
    def shouldHaveSameKey(expected: KeyValue): Unit = {
      actual shouldBe expected.key
    }
  }

  def getStats(keyValue: KeyValue): Option[Stats] =
    keyValue match {
      case _: KeyValue.ReadOnly =>
        None
      case keyValue: KeyValue.WriteOnly =>
        Some(keyValue.stats)
    }

  implicit class KeyValueImplicits(actual: KeyValue) {

    def toMemory: Memory =
      actual match {
        case readOnly: ReadOnly => readOnly.toMemory
        case writeOnly: WriteOnly => writeOnly.toMemory
      }

    def toMemoryResponse: Memory.Response =
      actual match {
        case readOnly: ReadOnly => readOnly.toMemoryResponse
        case writeOnly: WriteOnly => writeOnly.toMemoryResponse
      }

    def shouldBe(expected: KeyValue): Unit = {
      val actualMemory = actual.toMemory
      val expectedMemory = expected.toMemory

      (actualMemory, expectedMemory) match {
        case (actual: Memory.Group, expected: Memory.Group) =>
          actual.segmentCache.getAll().assertGet shouldBe expected.segmentCache.getAll().assertGet
        case _ =>
          actual.toMemory should be(expected.toMemory)
      }
    }
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
      expected match {
        case expectedRange: KeyValue.WriteOnly.Range =>
          actual should be(a[Persistent.Range])
          val actualRange = actual.asInstanceOf[Persistent.Range]
          actualRange.fromKey shouldBe expectedRange.fromKey
          actualRange.toKey shouldBe expectedRange.toKey
          actualRange.fetchFromAndRangeValue.assertGet shouldBe expectedRange.fetchFromAndRangeValue.assertGet
          actualRange.fetchFromValue.assertGetOpt shouldBe expectedRange.fetchFromValue.assertGetOpt
          actualRange.fetchRangeValue.assertGet shouldBe expectedRange.fetchRangeValue.assertGet

        case expectedGroup: KeyValue.WriteOnly.Group =>
          actual should be(a[Persistent.Group])
          actual.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe expectedGroup.keyValues

        case expected: KeyValue.WriteOnly.Fixed =>
          actual.key shouldBe expected.key
          actual.getOrFetchValue.assertGetOpt shouldBe expected.getOrFetchValue.assertGetOpt
          actual.asInstanceOf[KeyValue.ReadOnly.Fixed].deadline shouldBe expected.deadline
      }

    }
  }

  implicit class PersistentReadOnlyImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) = {
      expected match {
        case expectedRange: KeyValue.WriteOnly.Range =>
          actual should be(a[Persistent.Range])
          val actualRange = actual.asInstanceOf[Persistent.Range]
          actualRange.fromKey shouldBe expectedRange.fromKey
          actualRange.toKey shouldBe expectedRange.toKey
          actualRange.fetchFromValue.assertGetOpt shouldBe expectedRange.fetchFromValue.assertGetOpt
          actualRange.fetchRangeValue.assertGet shouldBe expectedRange.fetchRangeValue.assertGet

        case expected: KeyValue.WriteOnly.Fixed =>
          actual.key shouldBe expected.key
          actual.getOrFetchValue.assertGetOpt shouldBe expected.getOrFetchValue.assertGetOpt
          actual.asInstanceOf[KeyValue.WriteOnly.Fixed].deadline shouldBe expected.deadline
      }
    }
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

  implicit class MapEntryImplicits(actual: MapEntry[Slice[Byte], Memory.Response]) {

    def shouldBe(expected: MapEntry[Slice[Byte], Memory.Response]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual.totalByteSize shouldBe expected.totalByteSize
      actual match {
        case MapEntry.Put(key, value) =>
          val exp = expected.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Response]]
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
        lower.getOrFetchValue.assertGetOpt shouldBe expectedLowerKeyValue.getOrFetchValue.assertGetOpt
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   level: LevelZeroRef) = {
    @tailrec
    def assertHigher(index: Int) {
      //      println(s"assertLowers : ${index}")
      val lastIndex = keyValues.size - 1
      if (index > keyValues.size - 1) {
        //end
      } else if (index == lastIndex) {
        level.higher(keyValues(lastIndex).key).assertGetOpt shouldBe empty
        assertHigher(index + 1)
      } else {
        val expectedHigherKeyValue = keyValues(index + 1)
        val (higherKey, higherValue) = level.higher(keyValues(index).key).assertGet
        higherKey shouldBe expectedHigherKeyValue.key
        higherValue shouldBe expectedHigherKeyValue.getOrFetchValue.assertGetOpt
        assertHigher(index + 1)
      }
    }

    assertHigher(0)
  }

  def assertLower(keyValues: Slice[KeyValue],
                  level: LevelZeroRef) = {

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        level.lower(keyValues(0).key).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)

        val (lowerKey, lowerValue) = level.lower(keyValues(index).key).assertGet
        lowerKey shouldBe expectedLowerKeyValue.key
        lowerValue shouldBe expectedLowerKeyValue.getOrFetchValue.assertGetOpt
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertGet(keyValues: Slice[KeyValue.WriteOnly],
                reader: Reader) =
    keyValues foreach {
      keyValue =>
        SegmentReader.find(KeyMatcher.Get(keyValue.key), None, reader.copy()).assertGet shouldBe keyValue
    }

  def assertBloom(keyValues: Slice[KeyValue],
                  bloom: BloomFilter[Slice[Byte]]) =
    keyValues foreach {
      keyValue =>
        bloom.mightContain(keyValue.key) shouldBe true
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
        actual.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertReads(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader) = {

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
        level.get(keyValue.key).assertGet shouldBe keyValue
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe empty
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelZero) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe None
    }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key).assertGetOpt shouldBe empty
    }

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelZeroRef) =
    unzipGroups(keyValues) foreach {
      keyValue =>
        level.get(keyValue.key).assertGet shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertHeadLast(keyValues: Iterable[KeyValue],
                     zero: LevelZeroRef) = {
    val (headKey, headValue) = zero.head.assertGet
    headKey shouldBe keyValues.head.key
    headValue shouldBe keyValues.head.getOrFetchValue.assertGetOpt

    zero.headKey.assertGet shouldBe keyValues.head.key

    val (lastKey, lastValue) = zero.last.assertGet
    lastKey shouldBe keyValues.last.key
    lastValue shouldBe keyValues.last.getOrFetchValue.assertGetOpt
    zero.lastKey.assertGet shouldBe keyValues.last.key
  }

  def assertLower(keyValues: Slice[KeyValue.WriteOnly],
                  reader: Reader) = {

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
                   reader: Reader): Unit =
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

  def unzipGroups[T](keyValues: Iterable[T]): Slice[T] =
    keyValues.flatMap {
      case keyValue: KeyValue.WriteOnly.Group =>
        unzipGroups(keyValue.keyValues)
      case keyValue: KeyValue.ReadOnly.Group =>
        unzipGroups(keyValue.segmentCache.getAll().assertGet)
      case keyValue: KeyValue =>
        Slice(keyValue)
    }.toSlice.asInstanceOf[Slice[T]]

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
              if (next.key equiv range.toKey)
                shouldBeNextNext(toKeyHigher)
              else
                toKeyHigher.assertGet shouldBe next
          }

        case group: KeyValue.ReadOnly.Group if group.minKey != group.maxKey =>
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

  def randomIntKeyValuesMemory(count: Int = 5,
                               startId: Option[Int] = None,
                               valueSize: Option[Int] = None,
                               nonValue: Boolean = false,
                               addRandomRemoves: Boolean = false,
                               addRandomRanges: Boolean = false,
                               addRandomRemoveDeadlines: Boolean = false,
                               addRandomPutDeadlines: Boolean = false): Slice[Memory.Response] =
    randomIntKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      nonValue = nonValue,
      addRandomRemoves = addRandomRemoves,
      addRandomRanges = addRandomRanges,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines
    ).toMemory.asInstanceOf[Slice[Memory.Response]]

  def expiredDeadline(): Deadline = {
    val deadline = 0.nanosecond.fromNow - 100.millisecond
    deadline.hasTimeLeft() shouldBe false
    deadline
  }

  def assertGroup(group: Transient.Group,
                  expectedIndexCompressionUsed: CompressionInternal,
                  expectedValueCompressionUsed: Option[CompressionInternal]) = {
    val groupKeyValues = group.keyValues
    //check if there are values exists in the group's key-values. Use this flag to read values bytes.
    val hasNoValues = groupKeyValues.forall(_.getOrFetchValue.assertGetOpt.isEmpty)
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
      bloomFilterItemsCount = groupKeyValues.last.stats.bloomFilterItemsCount,
      bloomFilter = None
    )

    //read just the group bytes.
    val keyValues = SegmentReader.readAll(footer = tempFooter, reader = Reader(keyValuesDecompressedBytes)).assertGet

    keyValues should have size groupKeyValues.size
    keyValues shouldBe groupKeyValues

    //now write the Group to a full Segment and read it as a normal Segment.
    val (segmentBytes, nearestDeadline) = SegmentWriter.write(Seq(group.updateStats(0.1, None)), 0.1).assertGet
    nearestDeadline shouldBe Segment.getNearestDeadline(groupKeyValues).assertGetOpt
    val segmentReader = Reader(segmentBytes)
    val footer = SegmentReader.readFooter(segmentReader).assertGet
    SegmentReader.readAll(footer, segmentReader.copy()).assertGet shouldBe Seq(group)
  }

  def readAll(bytes: Slice[Byte]): Try[Slice[KeyValue.ReadOnly]] =
    readAll(Reader(bytes))

  def readAll(reader: Reader): Try[Slice[KeyValue.ReadOnly]] =
    SegmentReader.readFooter(reader) flatMap {
      footer =>
        SegmentReader.readAll(footer, reader.copy())
    }

  def printGroupHierarchy(keyValues: Slice[KeyValue.ReadOnly], spaces: Int): Unit =
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

  def openGroups(keyValues: Slice[KeyValue.ReadOnly]): Slicer[KeyValue.ReadOnly] =
    keyValues flatMap {
      case group: KeyValue.ReadOnly.Group =>
        openGroup(group)

      case keyValue =>
        Slice(keyValue)
    }

  def openGroup(group: KeyValue.ReadOnly.Group): Slicer[KeyValue.ReadOnly] = {
    val allKeyValues = group.segmentCache.getAll().assertGet
    allKeyValues flatMap {
      case group: KeyValue.ReadOnly.Group =>
        openGroup(group)

      case keyValue =>
        Slice(keyValue)
    }
  }
}
