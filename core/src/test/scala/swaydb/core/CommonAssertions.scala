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

import java.nio.file.{Path, Paths}

import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.exceptions.TestFailedException
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Memory.PendingApply
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.io.file.IOEffect
import swaydb.core.io.reader.Reader
import swaydb.core.level.zero.{LevelZero, LevelZeroSkipListMerger}
import swaydb.core.level.{Level, LevelRef, NextLevel}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryWriter, RangeValueSerializer, ValueSerializer}
import swaydb.core.merge._
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.SegmentBlock.SegmentBlockOps
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.Collections._
import swaydb.core.util.{Benchmark, SkipList}
import swaydb.data.config.IOStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.StorageUnits._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Random, Try}
import swaydb.serializers.Default._
import swaydb.serializers._

object CommonAssertions {

  implicit class KeyValueImplicits(actual: KeyValue) {

    def asPut: Option[KeyValue.ReadOnly.Put] =
      actual match {
        case keyValue: KeyValue.ReadOnly.Put =>
          Some(keyValue)

        case keyValue: Transient.Put =>
          Some(keyValue.toMemory.asInstanceOf[Memory.Put])

        case range: KeyValue.ReadOnly.Range =>
          range.fetchFromValue.right.value flatMap {
            case put: Value.Put =>
              Some(put.toMemory(range.fromKey))
            case _ =>
              None
          }

        case range: Transient.Range =>
          range.fromValue flatMap {
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
        case writeOnly: Transient => writeOnly.toMemory
      }

    def toMemoryResponse: Memory =
      actual match {
        case readOnly: ReadOnly => readOnly.toMemoryResponse
        case writeOnly: Transient => writeOnly.toMemoryResponse
      }

    def shouldBe(expected: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                     memorySweeper: Option[MemorySweeper.KeyValue] = TestLimitQueues.someMemorySweeper,
                                     segmentIO: SegmentIO = SegmentIO.random): Unit = {
      val actualMemory = actual.toMemory
      val expectedMemory = expected.toMemory

      actualMemory should be(expectedMemory)
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
              Some(keyValue.getOrFetchFunction.right.value)
            case keyValue: Memory.PendingApply =>
              val bytes = Slice.create[Byte](ValueSerializer.bytesRequired(keyValue.getOrFetchApplies.runRandomIO.right.value))
              ValueSerializer.write(keyValue.getOrFetchApplies.runRandomIO.right.value)(bytes)
              Some(bytes)
            case keyValue: Memory.Remove =>
              None
            case keyValue: Memory.Range =>
              val bytes = Slice.create[Byte](RangeValueSerializer.bytesRequired(keyValue.fromValue, keyValue.rangeValue))
              RangeValueSerializer.write(keyValue.fromValue, keyValue.rangeValue)(bytes)
              Some(bytes)

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
          }
        case keyValue: Persistent =>
          keyValue match {
            case keyValue: Persistent.Put =>
              keyValue.getOrFetchValue.runRandomIO.right.value

            case keyValue: Persistent.Update =>
              keyValue.getOrFetchValue.runRandomIO.right.value

            case keyValue: Persistent.Function =>
              Some(keyValue.getOrFetchFunction.runRandomIO.right.value)

            case keyValue: Persistent.PendingApply =>
              keyValue.toTransient.getOrFetchValue

            case keyValue: Persistent.Remove =>
              keyValue.toTransient.getOrFetchValue

            case keyValue: Persistent.Range =>
              keyValue.toTransient.getOrFetchValue
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

  def orNone[T](option: => Option[T]): Option[T] =
    if (Random.nextBoolean())
      None
    else
      option

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

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T, five: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four, () => five)).head()

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T, five: => T, six: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four, () => five, () => six)).head()

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

  implicit class PrintSkipList(skipList: SkipList.Concurrent[Slice[Byte], Memory]) {

    import KeyOrder.default._

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

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly],
                          oldKeyValues: Iterable[KeyValue.ReadOnly],
                          expected: Transient): SkipList.Concurrent[Slice[Byte], Memory] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected))

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue.ReadOnly],
                          oldKeyValues: Iterable[KeyValue.ReadOnly],
                          expected: Iterable[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long): SkipList.Concurrent[Slice[Byte], Memory] = {
    val skipList = SkipList.concurrent[Slice[Byte], Memory]()(KeyOrder.default)
    (oldKeyValues ++ newKeyValues).map(_.toMemoryResponse) foreach (memory => LevelZeroSkipListMerger.insert(memory.key, memory, skipList))
    skipList.asScala.toList shouldBe expected.map(keyValue => (keyValue.key, keyValue.toMemory))
    skipList
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly,
                  oldKeyValue: KeyValue.ReadOnly,
                  expected: Slice[Transient],
                  isLastLevel: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]]): Iterable[Iterable[Transient]] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly],
                  oldKeyValues: Slice[KeyValue.ReadOnly],
                  expected: Slice[Transient],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Iterable[Transient]] = {
    val result =
      SegmentMerger.merge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        minSegmentSize = 10.mb,
        isLastLevel = isLastLevel,
        forInMemory = false,
        createdInLevel = 0,
        valuesConfig = expected.lastOption.map(_.valuesConfig) getOrElse ValuesBlock.Config.random,
        sortedIndexConfig = expected.lastOption.map(_.sortedIndexConfig) getOrElse SortedIndexBlock.Config.random,
        binarySearchIndexConfig = expected.lastOption.map(_.binarySearchIndexConfig) getOrElse BinarySearchIndexBlock.Config.random,
        hashIndexConfig = expected.lastOption.map(_.hashIndexConfig) getOrElse HashIndexBlock.Config.random,
        bloomFilterConfig = expected.lastOption.map(_.bloomFilterConfig) getOrElse BloomFilterBlock.Config.random,
        segmentIO = SegmentIO.random
      ).runRandomIO.right.value

    if (expected.size == 0) {
      result shouldBe empty
    } else {
      result should have size 1
      val ungrouped = result.head
      ungrouped should have size expected.size
      ungrouped.toMemory.toList should contain inOrderElementsOf expected.toMemory
    }
    result
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly,
                  oldKeyValue: KeyValue.ReadOnly,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: KeyValue.ReadOnly)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]]): Iterable[Iterable[Transient]] =
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), Slice(lastLevelExpect))

  def assertMerge(newKeyValue: KeyValue.ReadOnly,
                  oldKeyValue: KeyValue.ReadOnly,
                  expected: KeyValue.ReadOnly,
                  lastLevelExpect: Option[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                              timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.map(Slice(_)).getOrElse(Slice.empty))
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected))
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly],
                  oldKeyValues: Slice[KeyValue.ReadOnly],
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValues, oldKeyValues, expected.toTransient(), isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect.toTransient(), isLastLevel = true)
    //println("*** Skip list assert ***")
    assertSkipListMerge(newKeyValues, oldKeyValues, expected)
  }

  def assertMerge(newKeyValue: KeyValue.ReadOnly,
                  oldKeyValue: KeyValue.ReadOnly,
                  expected: Slice[KeyValue.ReadOnly],
                  lastLevelExpect: Slice[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]]): Iterable[Iterable[Transient]] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected.toTransient(), isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect.toTransient(), isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue.ReadOnly],
                  oldKeyValues: Slice[KeyValue.ReadOnly],
                  expected: Transient,
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Iterable[Transient]] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), isLastLevel)

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Remove,
                  oldKeyValue: Memory.Fixed,
                  expected: KeyValue.ReadOnly.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    RemoveMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Put,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    PutMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.PendingApply,
                  expected: KeyValue.ReadOnly.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Fixed,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.PendingApply,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FixedMerger(newKeyValue, oldKeyValue).runRandomIO.right.value shouldBe expected
    assertMerge(newKeyValue: KeyValue.ReadOnly, oldKeyValue: KeyValue.ReadOnly, expected, lastLevel)
    //todo merge with persistent
  }

  implicit class SliceKeyValueImplicits(actual: Iterable[KeyValue]) {
    def shouldBe(expected: Iterable[KeyValue]): Unit = {
      val unzipActual = actual
      val unzipExpected = expected
      unzipActual.size shouldBe unzipExpected.size
      unzipActual.zip(unzipExpected) foreach {
        case (left, right) =>
          left shouldBe right
      }
    }

    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory]]) =
    //LevelZero does not write Groups therefore this unzip is required.
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue.toMemoryResponse)
          mapEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class MemoryImplicits(actual: Iterable[Memory]) {
    def toMapEntry(implicit serializer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory]]) =
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory]]) {
        case (mapEntry, keyValue) =>
          val newEntry = MapEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue)
          mapEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class SegmentsImplicits(actual: Iterable[Segment]) {

    def shouldHaveSameKeyValuesAs(expected: Iterable[Segment]): Unit =
      Segment.getAllKeyValues(actual).runRandomIO.right.value shouldBe Segment.getAllKeyValues(expected).runRandomIO.right.value
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
      case keyValue: Transient =>
        Some(keyValue.stats)
    }

  implicit class StatsOptionImplicits(actual: Option[Stats]) {
    def shouldBe(expected: Option[Stats], ignoreValueOffset: Boolean = false) =
      if (actual.isDefined && expected.isDefined)
        actual.value shouldBe(expected.value, ignoreValueOffset)
  }

  implicit class PersistentReadOnlyOptionImplicits(actual: Option[Persistent]) {
    def shouldBe(expected: Option[Persistent]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }
  }

  implicit class PersistentReadOnlyKeyValueOptionImplicits(actual: Option[Persistent]) {
    def shouldBe(expected: Option[Transient]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }

    def shouldBe(expected: Transient) =
      actual.value shouldBe expected
  }

  implicit class PersistentReadOnlyKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: Transient) = {
      actual.toMemory shouldBe expected.toMemory
    }
  }

  implicit class PersistentReadOnlyImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) =
      actual.toMemory shouldBe expected.toMemory
  }

  implicit class PersistentPartialReadOnlyImplicits(actual: Persistent.Partial) {
    def shouldBe(expected: KeyValue) =
      actual.toPersistent.get.toMemory shouldBe expected.toMemory
  }

  implicit class SegmentImplicits(actual: Segment) {

    def shouldBe(expected: Segment): Unit = {
      actual.path shouldBe expected.path
      actual.segmentSize shouldBe expected.segmentSize
      actual.minKey shouldBe expected.minKey
      actual.maxKey shouldBe expected.maxKey
      actual.minMaxFunctionId shouldBe expected.minMaxFunctionId
      actual.getBloomFilterKeyValueCount().runRandomIO.right.value shouldBe expected.getBloomFilterKeyValueCount().runRandomIO.right.value
      actual.persistent shouldBe actual.persistent
      actual.existsOnDisk shouldBe expected.existsOnDisk
      assertReads(expected.getAll().runRandomIO.right.value, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValue]): Unit =
      keyValues.foreach {
        keyValue =>
          actual.get(keyValue.key).runRandomIO.right.value.value shouldBe keyValue
      }
  }

  implicit class MapEntryImplicits(actual: MapEntry[Slice[Byte], Memory]) {

    def shouldBe(expected: MapEntry[Slice[Byte], Memory]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual.totalByteSize shouldBe expected.totalByteSize
      actual match {
        case MapEntry.Put(key, value) =>
          val exp = expected.asInstanceOf[MapEntry.Put[Slice[Byte], Memory]]
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

      val actualMap = SkipList.concurrent[Slice[Byte], Segment]()(KeyOrder.default)
      actual.applyTo(actualMap)

      val expectedMap = SkipList.concurrent[Slice[Byte], Segment]()(KeyOrder.default)
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
    assertHigher(keyValues, getHigher = key => level.higher(key).runIO)
  }

  def assertLower(keyValuesIterable: Iterable[KeyValue],
                  level: LevelRef) = {
    val keyValues = keyValuesIterable.toArray

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        level.lower(keyValues(0).key).runRandomIO.right.value shouldBe empty
        assertLowers(index + 1)
      } else {
        try {
          val lower = level.lower(keyValues(index).key).runRandomIO.right.value

          val expectedLowerKeyValue =
            (0 until index).reverse collectFirst {
              case i if unexpiredPuts(Slice(keyValues(i))).nonEmpty =>
                keyValues(i)
            }

          if (lower.nonEmpty) {
            expectedLowerKeyValue shouldBe defined
            lower.get.key shouldBe expectedLowerKeyValue.get.key
            lower.get.getOrFetchValue.runRandomIO.right.value shouldBe expectedLowerKeyValue.get.getOrFetchValue
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

  def assertGet(keyValues: Slice[Transient],
                rawSegmentReader: Reader[swaydb.Error.Segment],
                segmentIO: SegmentIO = SegmentIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {
    val blocks = readBlocksFromReader(rawSegmentReader.copy()).get

    keyValues foreach {
      keyValue =>
        //        val key = keyValue.minKey.readInt()
        //        if (key % 100 == 0)
        //          println(s"Key: $key")
        SegmentSearcher.search(
          key = keyValue.key,
          start = None,
          end = None,
          keyValueCount = IO.Right(blocks.footer.keyValueCount),
          hashIndexReader = IO(blocks.hashIndexReader),
          binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
          sortedIndexReader = blocks.sortedIndexReader,
          valuesReader = blocks.valuesReader,
          hasRange = blocks.footer.hasRange,
          threadState = ???
        ).runRandomIO.right.value.value.toPersistent.get shouldBe keyValue
    }
  }

  def assertBloom(keyValues: Slice[Transient],
                  bloom: BloomFilterBlock.State) = {
    val bloomFilter = Block.unblock[BloomFilterBlock.Offset, BloomFilterBlock](bloom.bytes).get

    keyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          key = keyValue.key,
          reader = bloomFilter
        ).get
    } should be >= (keyValues.size * 0.90).toInt

    assertBloomNotContains(bloom)
  }

  def assertBloom(keyValues: Slice[Transient],
                  segment: Segment) = {
    keyValues.par.count {
      keyValue =>
        segment.mightContainKey(keyValue.key).runRandomIO.right.value
    } shouldBe keyValues.size

    if (segment.hasBloomFilter.right.value)
      assertBloomNotContains(segment)
  }

  def assertBloom(keyValues: Slice[Transient],
                  bloomFilterReader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]) = {
    val unzipedKeyValues = keyValues

    unzipedKeyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          key = keyValue.key,
          reader = bloomFilterReader.copy()
        ).get
    } shouldBe unzipedKeyValues.size

    assertBloomNotContains(bloomFilterReader)
  }

  def assertBloomNotContains(bloomFilterReader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]) =
    (1 to 1000).par.count {
      _ =>
        BloomFilterBlock.mightContain(randomBytesSlice(100), bloomFilterReader.copy()).runRandomIO.right.value
    } should be <= 300

  def assertBloomNotContains(segment: Segment) =
    if (segment.hasBloomFilter.right.value)
      (1 to 1000).par.count {
        _ =>
          segment.mightContainKey(randomBytesSlice(100)).runRandomIO.right.value
      } should be < 1000

  def assertBloomNotContains(bloom: BloomFilterBlock.State) =
    runThisParallel(1000.times) {
      val bloomFilter = Block.unblock[BloomFilterBlock.Offset, BloomFilterBlock](bloom.bytes).get
      BloomFilterBlock.mightContain(
        key = randomBytesSlice(randomIntMax(1000) min 100),
        reader = bloomFilter.copy()
      ).runRandomIO.right.value shouldBe false
    }

  def assertReads(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertAllSegmentsCreatedInLevel(level: Level) =
    level.segmentsInLevel() foreach (_.createdInLevel.runRandomIO.right.value shouldBe level.levelNumber)

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
          val actual = level.getFromThisLevel(keyValue.key).runRandomIO.right.value.value
          actual.getOrFetchValue shouldBe keyValue.getOrFetchValue
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
      () => level.head.runIO.get shouldBe empty,
      () => level.last.runIO.get shouldBe empty,
    ).runThisRandomlyInParallel

  def assertReads(keyValues: Slice[Transient],
                  segmentReader: Reader[swaydb.Error.Segment])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {

    //read fullIndex
    readAll(segmentReader.copy()).runRandomIO.right.value shouldBe keyValues
    //    //find each KeyValue using all Matchers
    assertGet(keyValues, segmentReader.copy())
    assertLower(keyValues, segmentReader.copy())
    assertHigher(keyValues, segmentReader.copy())
  }

  def assertGet(keyValues: Iterable[KeyValue],
                segment: Segment): Unit =
    runAssertGet(
      keyValues = keyValues,
      segment = segment,
      parallel = true
    )

  def assertGetSequential(keyValues: Iterable[KeyValue],
                          segment: Segment): Unit =
    runAssertGet(
      keyValues = keyValues,
      segment = segment,
      parallel = false
    )

  private def runAssertGet(keyValues: Iterable[KeyValue],
                           segment: Segment,
                           parallel: Boolean = true) = {
    val parallelKeyValues =
      if (parallel)
        keyValues.par
      else
        keyValues

    parallelKeyValues foreach {
      keyValue =>
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 1000 == 0)
        //          println("Get: " + intKey)
        try
          segment.get(keyValue.key).runRandomIO.right.value.value shouldBe keyValue
        catch {
          case exception: Exception =>
            println(s"Failed to get: ${keyValue.key.readInt()}")
            throw exception
        }
    }
  }

  def dump(segments: Iterable[Segment]): Iterable[String] =
    Seq(s"Segments: ${segments.size}") ++ {
      segments map {
        segment =>
          val stringInfos =
            segment.getAll().value map {
              keyValue =>
                keyValue.toMemory match {
                  case response: Memory =>
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
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key).runRandomIO.get match {
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
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key).runRandomIO.right.value shouldBe empty
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
    keyValues.par foreach {
      keyValue =>
        level.get(keyValue.key).runRandomIO.right.value shouldBe None
    }

  def assertGetNone(keys: Range,
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key)).runRandomIO.right.value shouldBe empty
    }

  def assertGetNone(keys: List[Int],
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key)).runRandomIO.right.value shouldBe empty
    }

  def assertGetNoneButLast(keyValues: Iterable[KeyValue],
                           level: LevelRef) = {
    keyValues.dropRight(1).par foreach {
      keyValue =>
        level.get(keyValue.key).runRandomIO.right.value shouldBe empty
    }

    keyValues
      .lastOption
      .map(_.key)
      .flatMap(level.get(_).runRandomIO.right.value.map(_.toMemory)) shouldBe keyValues.lastOption
  }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level) =
    keyValues foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key).runRandomIO.right.value shouldBe empty
    }

  /**
   * If all key-values are non put key-values then searching higher for each key-value
   * can result in a very long search time. Considering using shuffleTake which
   * randomly selects a batch to assert for None higher.
   */
  def assertHigherNone(keyValues: Iterable[KeyValue],
                       level: LevelRef,
                       shuffleTake: Option[Int] = None) = {
    val unzipedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          //          println(keyValue.key.readInt())
          level.higher(keyValue.key).runRandomIO.right.value shouldBe empty
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
    val unzipedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          level.lower(keyValue.key).runRandomIO.right.value shouldBe empty
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

  def assertLower(keyValues: Slice[Transient],
                  reader: Reader[swaydb.Error.Segment])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default) = {
    val blocks = readBlocksFromReader(reader.copy()).get

    @tailrec
    def assertLowers(index: Int) {
      //      println(s"assertLowers : ${index}")
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        keyValues(index) match {
          case range: Transient.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = None,
              end = None,
              keyValueCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).runRandomIO.right.value shouldBe empty

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = None,
                  end = None,
                  keyValueCount = IO.Right(blocks.footer.keyValueCount),
                  binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReader = blocks.valuesReader
                ).runRandomIO.right.value.value shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = None,
              end = None,
              keyValueCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).runRandomIO.right.value shouldBe empty
        }
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        keyValues(index) match {
          case range: Transient.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = None,
              end = None,
              keyValueCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).runRandomIO.right.value.value shouldBe expectedLowerKeyValue

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = None,
                  end = None,
                  keyValueCount = IO.Right(blocks.footer.keyValueCount),
                  binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReader = blocks.valuesReader
                ).runRandomIO.right.value.value shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = None,
              end = None,
              keyValueCount = IO.Right(blocks.footer.keyValueCount),
              binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).runRandomIO.right.value.value shouldBe expectedLowerKeyValue
        }

        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   reader: Reader[swaydb.Error.Segment])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Unit = {
    val blocks = readBlocksFromReader(reader).get
    assertHigher(
      keyValues,
      getHigher =
        key =>
          SegmentSearcher.searchHigher(
            key = key,
            start = None,
            end = None,
            keyValueCount = IO.Right(blocks.footer.keyValueCount),
            binarySearchIndexReader = IO(blocks.binarySearchIndexReader),
            sortedIndexReader = blocks.sortedIndexReader,
            valuesReader = blocks.valuesReader
          ) flatMap {
            case Some(partial) =>
              partial.toPersistent.toOptionValue

            case None =>
              IO.none
          }
    )
  }

  def assertLower(keyValues: Slice[KeyValue],
                  segment: Segment) = {

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        //        println(s"Lower: ${actualKeyValue.key.readInt()}")
        segment.lower(actualKeyValue.key).runRandomIO.right.value shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 100 == 0)
        //          println(s"Lower: $intKey")
        try {
          val lower = segment.lower(keyValue.key).runRandomIO.right.value.value
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

  def assertHigher(keyValues: Slice[KeyValue],
                   segment: Segment): Unit =
    assertHigher(keyValues, getHigher = key => IO(segment.higher(key).runRandomIO.right.value))

  /**
   * Asserts that all key-values are returned in order when fetching higher in sequence.
   */
  def assertHigher(_keyValues: Iterable[KeyValue],
                   getHigher: Slice[Byte] => IO[swaydb.Error.Level, Option[KeyValue]]): Unit = {
    import KeyOrder.default._
    val keyValues = _keyValues.toMemory.toArray

    //assert higher if the currently's read key-value is the last key-value
    def assertLast(keyValue: KeyValue) =
      keyValue match {
        case range: KeyValue.ReadOnly.Range =>
          getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
          getHigher(range.toKey).runRandomIO.right.value shouldBe empty

        case keyValue =>
          getHigher(keyValue.key).runRandomIO.right.value shouldBe empty
      }

    //assert higher if the currently's read key-value is NOT the last key-value
    def assertNotLast(keyValue: KeyValue,
                      next: KeyValue,
                      nextNext: Option[KeyValue]) = {
      keyValue match {
        case range: KeyValue.ReadOnly.Range =>
          try
            getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
          catch {
            case exception: Exception =>
              exception.printStackTrace()
              getHigher(range.fromKey).runRandomIO.right.value.value shouldBe range
              throw exception
          }
          val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
          //suppose this keyValue is Range (1 - 10), second is Put(10), third is Put(11), higher on Range's toKey(10) will return 11 and not 10.
          //but 10 will be return if the second key-value was a range key-value.
          //if the toKey is equal to expected higher's key, then the higher is the next 3rd key.
          next match {
            case next: KeyValue.ReadOnly.Range =>
              toKeyHigher.value shouldBe next

            case _ =>
              //if the range's toKey is the same as next key, higher is next's next.
              //or if the next is group then
              if (next.key equiv range.toKey)
              //should be next next
                if (nextNext.isEmpty) //if there is no 3rd key, higher should be empty
                  toKeyHigher shouldBe empty
                else
                  try
                    toKeyHigher.value shouldBe nextNext.value
                  catch {
                    case exception: Exception =>
                      exception.printStackTrace()
                      val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
                      throw exception
                  }
              else
                try
                  toKeyHigher.value shouldBe next
                catch {
                  case exception: Exception =>
                    exception.printStackTrace()
                    val toKeyHigher = getHigher(range.toKey).runRandomIO.right.value
                    throw exception
                }
          }

        case _ =>
          Try(getHigher(keyValue.key).runRandomIO.right.value.value shouldBe next) recover {
            case _: TestFailedException =>
              unexpiredPuts(Slice(next)) should have size 0
          } get
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

  def readAll(closedSegment: SegmentBlock.Closed): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
    readAll(closedSegment.flattenSegmentBytes)

  def writeAndRead(keyValues: Iterable[Transient]): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] = {
    val segment = SegmentBlock.writeClosed(keyValues, 0, SegmentBlock.Config.random).get
    readAll(segment.flattenSegmentBytes)
  }

  def readBlocksFromSegment(closedSegment: SegmentBlock.Closed, segmentIO: SegmentIO = SegmentIO.random): IO[swaydb.Error.Segment, Blocks] =
    readBlocks(closedSegment.flattenSegmentBytes, segmentIO)

  def getBlocks(keyValues: Iterable[Transient], segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random, segmentIO: SegmentIO = SegmentIO.random): IO[swaydb.Error.Segment, Blocks] = {
    val closedSegment =
      SegmentBlock.writeClosed(
        keyValues = keyValues,
        segmentConfig = segmentConfig,
        createdInLevel = 0
      ).runRandomIO.right.value

    readBlocksFromSegment(closedSegment, segmentIO)
  }

  def readAll(bytes: Slice[Byte]): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
    readAll(Reader[swaydb.Error.Segment](bytes))

  def readBlocks(bytes: Slice[Byte], segmentIO: SegmentIO = SegmentIO.random): IO[swaydb.Error.Segment, Blocks] =
    readBlocksFromReader(Reader[swaydb.Error.Segment](bytes), segmentIO)

  def getSegmentBlockCache(keyValues: Slice[Transient], segmentIO: SegmentIO = SegmentIO.random, segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random): SegmentBlockCache = {
    val segment = SegmentBlock.writeClosed(keyValues, Int.MaxValue, segmentConfig = segmentConfig).get
    getSegmentBlockCacheFromSegmentClosed(segment, segmentIO)
  }

  def randomBlockSize(): Option[Int] =
    orNone(Some(4098))

  def randomIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                       includeReserved: Boolean = true): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries. =
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def getSegmentBlockCacheFromSegmentClosed(segment: SegmentBlock.Closed, segmentIO: SegmentIO = SegmentIO.random): SegmentBlockCache =
    SegmentBlockCache(
      id = "test",
      segmentIO = segmentIO,
      blockRef = BlockRefReader(segment.flattenSegmentBytes)
    )

  def getSegmentBlockCacheFromReader(reader: Reader[swaydb.Error.Segment], segmentIO: SegmentIO = SegmentIO.random): SegmentBlockCache =
    SegmentBlockCache(
      id = "test-cache",
      segmentIO = segmentIO,
      blockRef = BlockRefReader[SegmentBlock.Offset](reader.copy())(SegmentBlockOps).get
    )

  def readAll(reader: Reader[swaydb.Error.Segment]): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] = {
    val blockCache = getSegmentBlockCacheFromReader(reader)

    SortedIndexBlock
      .readAll(
        keyValueCount = blockCache.getFooter().get.keyValueCount,
        sortedIndexReader = blockCache.createSortedIndexReader().get,
        valuesReader = blockCache.createValuesReader().get
      )
  }

  def readBlocksFromReader(reader: Reader[swaydb.Error.Segment], segmentIO: SegmentIO = SegmentIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): IO[swaydb.Error.Segment, Blocks] = {
    val blockCache = getSegmentBlockCacheFromReader(reader, segmentIO)
    readBlocks(blockCache)
  }

  def readBlocks(blockCache: SegmentBlockCache) =
    IO {
      Blocks(
        footer = blockCache.getFooter().get,
        valuesReader = blockCache.createValuesReader().get,
        sortedIndexReader = blockCache.createSortedIndexReader().get,
        hashIndexReader = blockCache.createHashIndexReader().get,
        binarySearchIndexReader = blockCache.createBinarySearchIndexReader().get,
        bloomFilterReader = blockCache.createBloomFilterReader().get
      )
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
            FixedMerger(newer, older.toMemory(newKeyValue.key)).runRandomIO.right.value match {
              case newPendingApply: ReadOnly.PendingApply =>
                val resultApplies = newPendingApply.getOrFetchApplies.runRandomIO.right.value.reverse.toList ++ reveredApplied.drop(count)
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
    IO(assertSliced(keyValue)).left.runRandomIO.right.value

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
        }
      case persistent: Persistent =>
        persistent match {
          case Persistent.Remove(_key, deadline, _time, indexOffset, nextIndexOffset, nextIndexSize, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()

          case put @ Persistent.Put(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            put.getOrFetchValue.runRandomIO.right.value.shouldBeSliced()

          case updated @ Persistent.Update(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            updated.getOrFetchValue.runRandomIO.right.value.shouldBeSliced()

          case function @ Persistent.Function(_key, lazyFunctionReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            function.getOrFetchFunction.runRandomIO.right.value.shouldBeSliced()

          case pendingApply @ Persistent.PendingApply(_key, _time, deadline, lazyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            pendingApply.getOrFetchApplies.runRandomIO.right.value foreach assertSliced

          case range @ Persistent.Range(_fromKey, _toKey, lazyRangeValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _fromKey.shouldBeSliced()
            _toKey.shouldBeSliced()
            range.fetchFromValue.runRandomIO.right.value foreach assertSliced
            assertSliced(range.fetchRangeValue.runRandomIO.right.value)
        }
    }

  def countRangesManually(keyValues: Iterable[Transient]): Int =
    keyValues.foldLeft(0) {
      case (count, keyValue) =>
        keyValue match {
          case fixed: Transient.Fixed =>
            count
          case range: Transient.Range =>
            count + 1
        }
    }

  implicit class BooleanImplicit(bool: Boolean) {
    def toInt =
      if (bool) 1 else 0
  }

  implicit class SegmentIOImplicits(io: SegmentIO.type) {
    def random: SegmentIO =
      random(cacheOnAccess = randomBoolean())

    def random(cacheOnAccess: Boolean = randomBoolean(),
               includeReserved: Boolean = true): SegmentIO =
      SegmentIO(
        segmentBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        hashIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        bloomFilterBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        binarySearchIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        sortedIndexBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        valuesBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved),
        segmentFooterBlockIO = _ => randomIOStrategy(cacheOnAccess, includeReserved)
      )
  }
}
