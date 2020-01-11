/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import org.scalactic.Equality
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.words.EmptyWord
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.Memory.PendingApply
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{KeyValue, Memory, Value, _}
import swaydb.core.io.file.Effect
import swaydb.core.io.reader.Reader
import swaydb.core.level.zero.{LevelZero, LevelZeroSkipListMerger}
import swaydb.core.level.{Level, LevelRef, NextLevel}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryWriter, RangeValueSerializer, ValueSerializer}
import swaydb.core.merge._
import swaydb.core.segment.KeyMatcher.Result
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.SegmentBlock.SegmentBlockOps
import swaydb.core.segment.format.a.block.segment.data.TransientSegment
import swaydb.core.segment.format.a.block.segment.{SegmentBlock, SegmentBlockCache}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.segment.{KeyMatcher, Segment, SegmentIO, SegmentOptional, SegmentSearcher, ThreadReadState}
import swaydb.core.util.SkipList
import swaydb.data.config.IOStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice, SliceOptional}
import swaydb.data.util.SomeOrNone
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.util.{Random, Try}
import swaydb.data.util.SomeOrNone._

object CommonAssertions {

  implicit class KeyValueImplicits(actual: KeyValue) {

    def asPut: Option[KeyValue.Put] =
      actual match {
        case keyValue: KeyValue.Put =>
          Some(keyValue)

        case range: KeyValue.Range =>
          range.fetchFromValueUnsafe flatMapOptionS {
            case put: Value.Put =>
              Some(put.toMemory(range.fromKey))
            case _ =>
              None
          }

        case _ =>
          None
      }

    def shouldBe(expected: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeperMax,
                                     segmentIO: SegmentIO = SegmentIO.random): Unit = {
      val actualMemory = actual.toMemory
      val expectedMemory = expected.toMemory

      actualMemory should be(expectedMemory.unslice())
    }

    def getOrFetchValue: Option[Slice[Byte]] =
      actual match {
        case keyValue: Memory =>
          keyValue match {
            case keyValue: Memory.Put =>
              keyValue.value.toOptionC
            case keyValue: Memory.Update =>
              keyValue.value.toOptionC
            case keyValue: Memory.Function =>
              Some(keyValue.getOrFetchFunction)
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
        case keyValue: Persistent =>
          keyValue match {
            case keyValue: Persistent.Put =>
              keyValue.getOrFetchValue.runRandomIO.right.value.toOptionC

            case keyValue: Persistent.Update =>
              keyValue.getOrFetchValue.runRandomIO.right.value.toOptionC

            case keyValue: Persistent.Function =>
              Some(keyValue.getOrFetchFunction.runRandomIO.right.value)

            case keyValue: Persistent.PendingApply =>
              val applies = keyValue.getOrFetchApplies.runRandomIO.right.value

              applies.forall(_.isUnsliced) shouldBe true

              val bytes = Slice.create[Byte](ValueSerializer.bytesRequired(applies))
              ValueSerializer.write(applies)(bytes)
              Some(bytes)

            case keyValue: Persistent.Remove =>
              None

            case range: Persistent.Range =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValueUnsafe.runRandomIO.right.value
              fromValue.forallS(_.isUnsliced) shouldBe true
              rangeValue.isUnsliced shouldBe true

              val bytes = Slice.create[Byte](RangeValueSerializer.bytesRequired(fromValue, rangeValue))
              RangeValueSerializer.write(fromValue, rangeValue)(bytes)
              Some(bytes)
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
          range.fromValue flatMapOptionS {
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

  implicit class PrintSkipList(skipList: SkipList.Concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory]) {

    //stringify the skipList so that it's readable
    def asString(value: Value): String =
      value match {
        case Value.Remove(deadline, time) =>
          s"Remove(deadline = $deadline)"
        case Value.Put(value, deadline, time) =>
          s"Put(${value.toOptionC.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
        case Value.Update(value, deadline, time) =>
          s"Update(${value.toOptionC.map(_.read[Int]).getOrElse("None")}, deadline = $deadline)"
      }
  }

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Memory): SkipList.Concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory] =
    assertSkipListMerge(newKeyValues, oldKeyValues, Slice(expected))

  def assertSkipListMerge(newKeyValues: Iterable[KeyValue],
                          oldKeyValues: Iterable[KeyValue],
                          expected: Iterable[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long): SkipList.Concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory] = {
    val skipList = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(KeyOrder.default)
    (oldKeyValues ++ newKeyValues).map(_.toMemory) foreach (memory => LevelZeroSkipListMerger.insert(memory.key, memory, skipList))
    skipList.asScala.toList shouldBe expected.map(keyValue => (keyValue.key, keyValue.toMemory)).toList
    skipList
  }

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: Slice[Memory],
                  isLastLevel: Boolean = false)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel)

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] = {
    val builder = MergeStats.random()

    SegmentMerger.merge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = isLastLevel
    )

    val result = builder.keyValues

    if (expected.size == 0) {
      result.isEmpty shouldBe true
    } else {
      result should have size expected.size
      result.toList should contain inOrderElementsOf expected
    }
    result
  }

  //  def assertMerge(newKeyValue: KeyValue,
  //                  oldKeyValue: KeyValue,
  //                  expected: KeyValue,
  //                  lastLevelExpect: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]],
  //                                             timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
  //    assertMerge(newKeyValue, oldKeyValue, Slice(expected), Slice(lastLevelExpect))

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: KeyValue,
                  lastLevelExpect: KeyValueOptional)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValue, oldKeyValue, Slice(expected), lastLevelExpect.toOptional.map(Slice(_)).getOrElse(Slice.empty))
    //println("*** Skip list assert ***")
    assertSkipListMerge(Slice(newKeyValue), Slice(oldKeyValue), Slice(expected))
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Slice[KeyValue],
                  lastLevelExpect: Slice[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    //    println("*** Expected assert ***")
    assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = false)
    //println("*** Expected last level ***")
    assertMerge(newKeyValues, oldKeyValues, lastLevelExpect, isLastLevel = true)
    //println("*** Skip list assert ***")
    assertSkipListMerge(newKeyValues, oldKeyValues, expected)
  }

  def assertMerge(newKeyValue: KeyValue,
                  oldKeyValue: KeyValue,
                  expected: Slice[KeyValue],
                  lastLevelExpect: Slice[KeyValue])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] = {
    //    println("*** Last level = false ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), expected, isLastLevel = false)
    //println("*** Last level = true ***")
    assertMerge(Slice(newKeyValue), Slice(oldKeyValue), lastLevelExpect, isLastLevel = true)
  }

  def assertMerge(newKeyValues: Slice[KeyValue],
                  oldKeyValues: Slice[KeyValue],
                  expected: Memory,
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]]): Iterable[Memory] =
    assertMerge(newKeyValues, oldKeyValues, Slice(expected), isLastLevel)

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Function,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FunctionMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Remove,
                  oldKeyValue: Memory.Fixed,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    RemoveMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Put,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    PutMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.Fixed,
                  expected: Memory.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Update,
                  oldKeyValue: Memory.PendingApply,
                  expected: KeyValue.Fixed,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    UpdateMerger(newKeyValue, oldKeyValue) shouldBe expected
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))

    //todo merge with persistent
  }

  def assertMerge(newKeyValue: Memory.Fixed,
                  oldKeyValue: Memory.PendingApply,
                  expected: Memory.PendingApply,
                  lastLevel: Option[Memory.Fixed])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]]): Unit = {
    FixedMerger(newKeyValue, oldKeyValue) shouldBe expected
    assertMerge(newKeyValue: KeyValue, oldKeyValue: KeyValue, expected, lastLevel.getOrElse(Memory.Null))
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
          val newEntry = MapEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue.toMemory)
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

    def toPersistentMergeBuilder: MergeStats.Persistent.Builder[Memory, ListBuffer] =
      MergeStats.persistentBuilder(actual)

    def toMemoryMergeBuilder: MergeStats.Memory.Builder[Memory, ListBuffer] =
      MergeStats.memoryBuilder(actual)

    def toBufferMergeBuilder: MergeStats.Buffer[Memory, ListBuffer] =
      MergeStats.bufferBuilder(actual)

    def toMergeBuilder: MergeStats[Memory, ListBuffer] =
      MergeStats.randomBuilder(actual)
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

  implicit class PersistentKeyValueOptionImplicits(actual: PersistentOptional) {
    def shouldBe(expected: PersistentOptional): Unit = {
      actual.isSomeS shouldBe expected.isSomeS
      if (actual.isSomeS)
        actual.getS shouldBe expected.getS
    }
  }

  implicit class PersistentKeyValueKeyValueOptionImplicits(actual: PersistentOptional) {
    def shouldBe(expected: MemoryOptional) = {
      actual.isSomeS shouldBe expected.isSomeS
      if (actual.isSomeS)
        actual.getS shouldBe expected.getS
    }

    def shouldBe(expected: Memory): Unit =
      actual.getS shouldBe expected
  }

  implicit class PersistentKeyValueKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: Memory) = {
      actual.toMemory() shouldBe expected.toMemory
    }
  }

  implicit class PersistentKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) =
      actual.toMemory shouldBe expected.toMemory
  }

  implicit class SegmentImplicits(actual: Segment) {

    def shouldBe(expected: Segment): Unit = {
      actual.path shouldBe expected.path
      actual.segmentSize shouldBe expected.segmentSize
      actual.minKey shouldBe expected.minKey
      actual.maxKey shouldBe expected.maxKey
      actual.minMaxFunctionId shouldBe expected.minMaxFunctionId
      actual.getKeyValueCount().runRandomIO.right.value shouldBe expected.getKeyValueCount().runRandomIO.right.value
      actual.persistent shouldBe actual.persistent
      actual.existsOnDisk shouldBe expected.existsOnDisk
      actual.minMaxFunctionId shouldBe expected.minMaxFunctionId
      actual.segmentId shouldBe expected.segmentId
      assertReads(expected.toSlice().runRandomIO.right.value, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValue]): Unit =
      keyValues.foreach {
        keyValue =>
          actual.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.getUnsafe shouldBe keyValue
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

      val actualMap = SkipList.concurrent[SliceOptional[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
      actual.applyTo(actualMap)

      val expectedMap = SkipList.concurrent[SliceOptional[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
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
    assertHigher(keyValues, getHigher = key => IO.Defer(level.higher(key, ThreadReadState.random).toOptionPut).runIO)
  }

  def assertLower(keyValuesIterable: Iterable[KeyValue],
                  level: LevelRef) = {
    val keyValues = keyValuesIterable.toSlice

    @tailrec
    def assertLowers(index: Int): Unit = {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        level.lower(keyValues(0).key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
        assertLowers(index + 1)
      } else {
        try {
          val lower = level.lower(keyValues(index).key, ThreadReadState.random).runRandomIO.right.value.toOptionPut

          val expectedLowerKeyValue =
            (0 until index).reverse collectFirst {
              case i if unexpiredPuts(Slice(keyValues(i))).nonEmpty =>
                keyValues(i)
            }

          if (lower.nonEmpty) {
            expectedLowerKeyValue shouldBe defined
            lower.get.key shouldBe expectedLowerKeyValue.get.key
            lower.get.getOrFetchValue.runRandomIO.right.value shouldBe expectedLowerKeyValue.get.getOrFetchValue.asSliceOptional()
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

  def assertGet(keyValues: Slice[Memory],
                rawSegmentReader: Reader,
                segmentIO: SegmentIO = SegmentIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                         blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
    val blocks = readBlocksFromReader(rawSegmentReader.copy()).get

    keyValues.par foreach {
      keyValue =>
        //        val key = keyValue.minKey.readInt()
        //        if (key % 100 == 0)
        //          println(s"Key: $key")
        SegmentSearcher.searchRandom(
          key = keyValue.key,
          start = Persistent.Null,
          end = Persistent.Null,
          keyValueCount = blocks.footer.keyValueCount,
          hashIndexReaderOrNull = blocks.hashIndexReader.map(_.copy()).orNull,
          binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.map(_.copy()).orNull,
          sortedIndexReader = blocks.sortedIndexReader.copy(),
          valuesReaderOrNull = blocks.valuesReader.map(_.copy()).orNull,
          hasRange = blocks.footer.hasRange
        ).runRandomIO.right.value.getS shouldBe keyValue
    }
  }

  def assertBloom(keyValues: Slice[Memory],
                  bloom: BloomFilterBlock.State) = {
    val bloomFilter = Block.unblock[BloomFilterBlock.Offset, BloomFilterBlock](bloom.compressibleBytes)

    keyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          key = keyValue.key,
          reader = bloomFilter
        )
    } should be >= (keyValues.size * 0.90).toInt

    assertBloomNotContains(bloom)
  }

  def assertBloom(keyValues: Slice[KeyValue],
                  segment: Segment) = {
    keyValues.par.count {
      keyValue =>
        IO.Defer(segment.mightContainKey(keyValue.key)).runRandomIO.right.value
    } shouldBe keyValues.size

    if (segment.hasBloomFilter || segment.memory)
      assertBloomNotContains(segment)
  }

  def assertBloom(keyValues: Slice[Memory],
                  bloomFilterReader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]) = {
    val unzipedKeyValues = keyValues

    unzipedKeyValues.par.count {
      keyValue =>
        BloomFilterBlock.mightContain(
          key = keyValue.key,
          reader = bloomFilterReader.copy()
        )
    } shouldBe unzipedKeyValues.size

    assertBloomNotContains(bloomFilterReader)
  }

  def assertBloomNotContains(bloomFilterReader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]) =
    (1 to 1000).par.count {
      _ =>
        BloomFilterBlock.mightContain(randomBytesSlice(100), bloomFilterReader.copy()).runRandomIO.right.value
    } should be <= 300

  def assertBloomNotContains(segment: Segment) =
    if (segment.hasBloomFilter)
      (1 to 1000).par.count {
        _ =>
          segment.mightContainKey(randomBytesSlice(100)).runRandomIO.right.value
      } should be < 1000

  def assertBloomNotContains(bloom: BloomFilterBlock.State) =
    runThisParallel(1000.times) {
      val bloomFilter = Block.unblock[BloomFilterBlock.Offset, BloomFilterBlock](bloom.compressibleBytes)
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
          val actual = level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.right.value.getUnsafe
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
      () => IO.Defer(level.head(ThreadReadState.random).toOptionPut).runIO.get shouldBe empty,
      () => IO.Defer(level.last(ThreadReadState.random).toOptionPut).runIO.get shouldBe empty
    ).runThisRandomlyInParallel

  def assertReads(keyValues: Slice[Memory],
                  segmentReader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                         blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {

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
          IO.Defer(segment.get(keyValue.key, ThreadReadState.random)).runRandomIO.value.getUnsafe shouldBe keyValue
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
            segment.toSlice() map {
              keyValue =>
                keyValue.toMemory match {
                  case response: Memory =>
                    response match {
                      case fixed: Memory.Fixed =>
                        fixed match {
                          case Memory.Put(key, value, deadline, time) =>
                            s"""PUT - ${key.readInt()} -> ${value.toOptionC.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

                          case Memory.Update(key, value, deadline, time) =>
                            s"""UPDATE - ${key.readInt()} -> ${value.toOptionC.map(_.readInt())}, ${deadline.map(_.hasTimeLeft())}, ${time.time.readLong()}"""

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
                        s"""RANGE - ${fromKey.readInt()} -> ${toKey.readInt()}, $fromValue (${fromValue.toOptionS.map(Value.hasTimeLeft)}), $rangeValue (${Value.hasTimeLeft(rangeValue)})"""
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
        Effect.write(Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"), Slice(Slice.writeString(data.mkString("\n"))))

        dump(nextLevel)

      case None =>
        val data =
          Seq(s"\nLevel: ${level.rootPath}\n") ++
            dump(level.segmentsInLevel())
        Effect.write(Paths.get(s"/Users/simerplaha/IdeaProjects/SwayDB/core/target/dump_Level_${level.levelNumber}.txt"), Slice(Slice.writeString(data.mkString("\n"))))
    }

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelRef) =
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut match {
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
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
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
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe None
    }

  def assertGetNone(keys: Range,
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

  def assertGetNone(keys: List[Int],
                    level: LevelRef) =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

  def assertGetNoneButLast(keyValues: Iterable[KeyValue],
                           level: LevelRef) = {
    keyValues.dropRight(1).par foreach {
      keyValue =>
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
    }

    keyValues
      .lastOption
      .map(_.key)
      .flatMap(level.get(_, ThreadReadState.random).runRandomIO.right.value.toOptionPut.map(_.toMemory)) shouldBe keyValues.lastOption
  }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level) =
    keyValues foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptional shouldBe empty
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
          level.higher(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
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
          level.lower(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
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

  def assertLower(keyValues: Slice[Memory],
                  reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                  blockCacheMemorySweeper: Option[MemorySweeper.Block]) = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

    val blocks = readBlocksFromReader(reader.copy()).get

    @tailrec
    def assertLowers(index: Int): Unit = {
      //      println(s"assertLowers : ${index}")
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        keyValues(index) match {
          case range: Memory.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.toOptional shouldBe empty

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = Persistent.Null,
                  end = Persistent.Null,
                  keyValueCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ).runRandomIO.right.value.getUnsafe shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.toOptional shouldBe empty
        }
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        keyValues(index) match {
          case range: Memory.Range =>
            SegmentSearcher.searchLower(
              key = range.fromKey,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.getUnsafe shouldBe expectedLowerKeyValue

            (range.fromKey.readInt() + 1 to range.toKey.readInt()) foreach {
              key =>
                SegmentSearcher.searchLower(
                  key = Slice.writeInt(key),
                  start = Persistent.Null,
                  end = Persistent.Null,
                  keyValueCount = blocks.footer.keyValueCount,
                  binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
                  sortedIndexReader = blocks.sortedIndexReader,
                  valuesReaderOrNull = blocks.valuesReader.orNull
                ).runRandomIO.right.value.getUnsafe shouldBe range
            }

          case _ =>
            SegmentSearcher.searchLower(
              key = keyValues(index).key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.orNull,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderOrNull = blocks.valuesReader.orNull
            ).runRandomIO.right.value.getUnsafe shouldBe expectedLowerKeyValue
        }

        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValue],
                   reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                   blockCacheMemorySweeper: Option[MemorySweeper.Block]): Unit = {
    implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
    val blocks = readBlocksFromReader(reader).get
    assertHigher(
      keyValues,
      getHigher =
        key =>
          IO {
            SegmentSearcher.searchHigherRandomly(
              key = key,
              start = Persistent.Null,
              end = Persistent.Null,
              keyValueCount = blocks.footer.keyValueCount,
              binarySearchIndexReaderOrNull = blocks.binarySearchIndexReader.map(_.copy()).orNull,
              sortedIndexReader = blocks.sortedIndexReader.copy(),
              valuesReaderOrNull = blocks.valuesReader.map(_.copy()).orNull
            ).toOptional
          }
    )
  }

  def assertLower(keyValues: Slice[KeyValue],
                  segment: Segment) = {

    @tailrec
    def assertLowers(index: Int): Unit = {
      if (index > keyValues.size - 1) {
        //end
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        //        println(s"Lower: ${actualKeyValue.key.readInt()}")
        IO.Defer(segment.lower(actualKeyValue.key, ThreadReadState.random)).runRandomIO.right.value.toOptional shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        //        val intKey = keyValue.key.readInt()
        //        if (intKey % 100 == 0)
        //          println(s"Lower: $intKey")
        try {
          val lower = IO.Defer(segment.lower(keyValue.key, ThreadReadState.random)).runRandomIO.right.value.getUnsafe
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
    assertHigher(keyValues, getHigher = key => IO(IO.Defer(segment.higher(key, ThreadReadState.random)).runRandomIO.right.value.toOptional))

  /**
   * Asserts that all key-values are returned in order when fetching higher in sequence.
   */
  def assertHigher(_keyValues: Iterable[KeyValue],
                   getHigher: Slice[Byte] => IO[swaydb.Error.Level, Option[KeyValue]]): Unit = {
    import KeyOrder.default._
    val keyValues = _keyValues.toArray

    //assert higher if the currently's read key-value is the last key-value
    def assertLast(keyValue: KeyValue) =
      keyValue match {
        case range: KeyValue.Range =>
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
        case range: KeyValue.Range =>
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
            case next: KeyValue.Range =>
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
    if (randomBoolean())
      None
    else
      Some(expiredDeadline())

  def readAll(segment: TransientSegment.One)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    readAll(segment.flattenSegmentBytes)

  def writeAndRead(keyValues: Iterable[Memory])(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] = {
    val sortedIndexBlock = SortedIndexBlock.Config.random

    val segment =
      SegmentBlock.writeOnes(
        mergeStats = MergeStats.persistentBuilder(keyValues).close(sortedIndexBlock.enableAccessPositionIndex),
        createdInLevel = 0,
        bloomFilterConfig = BloomFilterBlock.Config.random,
        hashIndexConfig = HashIndexBlock.Config.random,
        binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
        sortedIndexConfig = sortedIndexBlock,
        valuesConfig = ValuesBlock.Config.random,
        segmentConfig = SegmentBlock.Config.random.copy(Int.MaxValue)
      )

    segment should have size 1

    readAll(segment.head.flattenSegmentBytes)
  }

  def readBlocksFromSegment(closedSegment: TransientSegment.One,
                            segmentIO: SegmentIO = SegmentIO.random,
                            useCacheableReaders: Boolean = randomBoolean())(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] =
    if (useCacheableReaders && closedSegment.sortedIndexUnblockedReader.isDefined && randomBoolean()) //randomly also use cacheable readers
      IO(readCachedBlocksFromSegment(closedSegment).get)
    else
      readBlocks(closedSegment.flattenSegmentBytes, segmentIO)

  def readCachedBlocksFromSegment(closedSegment: TransientSegment.One): Option[SegmentBlocks] =
    if (closedSegment.sortedIndexUnblockedReader.isDefined)
      Some(
        SegmentBlocks(
          valuesReader = closedSegment.valuesUnblockedReader,
          sortedIndexReader = closedSegment.sortedIndexUnblockedReader.get,
          hashIndexReader = closedSegment.hashIndexUnblockedReader,
          binarySearchIndexReader = closedSegment.binarySearchUnblockedReader,
          bloomFilterReader = closedSegment.bloomFilterUnblockedReader,
          footer = closedSegment.footerUnblocked.get
        )
      )
    else
      None

  def getBlocks(keyValues: Iterable[Memory],
                useCacheableReaders: Boolean = randomBoolean(),
                valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
                sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
                binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
                hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
                bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
                segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[Error.Segment, Slice[SegmentBlocks]] = {
    val closedSegments =
      SegmentBlock.writeOnes(
        mergeStats = MergeStats.persistentBuilder(keyValues).close(sortedIndexConfig.enableAccessPositionIndex),
        createdInLevel = 0,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )

    import IO._
    import swaydb.Error.Segment.ExceptionHandler

    val segmentIO =
      SegmentIO(
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )

    closedSegments.mapRecoverIO {
      closedSegment =>
        readBlocksFromSegment(
          useCacheableReaders = useCacheableReaders,
          closedSegment = closedSegment,
          segmentIO = segmentIO
        )
    }
  }

  def getBlocksSingle(keyValues: Iterable[Memory],
                      valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
                      sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
                      binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
                      hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
                      bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
                      segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[Error.Segment, SegmentBlocks] =
    getBlocks(
      keyValues = keyValues,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig.copy(Int.MaxValue)
    ) map {
      segments =>
        segments should have size 1
        segments.head
    }

  def readAll(bytes: Slice[Byte])(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    readAll(Reader(bytes))

  def readBlocks(bytes: Slice[Byte],
                 segmentIO: SegmentIO = SegmentIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] =
    readBlocksFromReader(Reader(bytes), segmentIO)

  def getSegmentBlockCache(keyValues: Slice[Memory],
                           valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
                           sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
                           binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
                           hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
                           bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
                           segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): Iterable[SegmentBlockCache] =
    SegmentBlock.writeOnes(
      mergeStats = MergeStats.persistentBuilder(keyValues).close(sortedIndexConfig.enableAccessPositionIndex),
      createdInLevel = Int.MaxValue,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    ) map {
      closed =>
        val segmentIO =
          SegmentIO(
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )

        getSegmentBlockCacheFromSegmentClosed(closed, segmentIO)
    }

  def getSegmentBlockCacheSingle(keyValues: Slice[Memory],
                                 valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random,
                                 sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random,
                                 binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random,
                                 hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random,
                                 bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random,
                                 segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): SegmentBlockCache = {
    val blockCaches =
      getSegmentBlockCache(
        keyValues = keyValues,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig.copy(Int.MaxValue)
      )

    blockCaches should have size 1
    blockCaches.head
  }

  def randomBlockSize(): Option[Int] =
    orNone(Some(4098))

  def randomIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                       includeReserved: Boolean = true): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (cacheOnAccess && includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def randomIOStrategyWithCacheOnAccess(cacheOnAccess: Boolean): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = cacheOnAccess) //not used in stress tests.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def getSegmentBlockCacheFromSegmentClosed(segment: TransientSegment.One,
                                            segmentIO: SegmentIO = SegmentIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): SegmentBlockCache =
    SegmentBlockCache(
      path = Paths.get("test"),
      segmentIO = segmentIO,
      blockRef = BlockRefReader(segment.flattenSegmentBytes),
      valuesReaderCacheable = segment.valuesUnblockedReader,
      sortedIndexReaderCacheable = segment.sortedIndexUnblockedReader,
      hashIndexReaderCacheable = segment.hashIndexUnblockedReader,
      binarySearchIndexReaderCacheable = segment.binarySearchUnblockedReader,
      bloomFilterReaderCacheable = segment.bloomFilterUnblockedReader,
      footerCacheable = segment.footerUnblocked
    )

  def getSegmentBlockCacheFromReader(reader: Reader,
                                     segmentIO: SegmentIO = SegmentIO.random)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): SegmentBlockCache =
    SegmentBlockCache(
      path = Paths.get("test-cache"),
      segmentIO = segmentIO,
      blockRef = BlockRefReader[SegmentBlock.Offset](reader.copy())(SegmentBlockOps),
      valuesReaderCacheable = None,
      sortedIndexReaderCacheable = None,
      hashIndexReaderCacheable = None,
      binarySearchIndexReaderCacheable = None,
      bloomFilterReaderCacheable = None,
      footerCacheable = None
    )

  def readAll(reader: Reader)(implicit blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, Slice[KeyValue]] =
    IO {
      val blockCache = getSegmentBlockCacheFromReader(reader)

      SortedIndexBlock
        .toSlice(
          keyValueCount = blockCache.getFooter().keyValueCount,
          sortedIndexReader = blockCache.createSortedIndexReader(),
          valuesReaderOrNull = blockCache.createValuesReaderOrNull()
        )
    }

  def readBlocksFromReader(reader: Reader, segmentIO: SegmentIO = SegmentIO.random)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                    blockCacheMemorySweeper: Option[MemorySweeper.Block]): IO[swaydb.Error.Segment, SegmentBlocks] = {
    val blockCache = getSegmentBlockCacheFromReader(reader, segmentIO)
    readBlocks(blockCache)
  }

  def readBlocks(blockCache: SegmentBlockCache) =
    IO {
      SegmentBlocks(
        footer = blockCache.getFooter(),
        valuesReader = Option(blockCache.createValuesReaderOrNull()),
        sortedIndexReader = blockCache.createSortedIndexReader(),
        hashIndexReader = Option(blockCache.createHashIndexReaderOrNull()),
        binarySearchIndexReader = Option(blockCache.createBinarySearchIndexReaderOrNull()),
        bloomFilterReader = Option(blockCache.createBloomFilterReaderOrNull())
      )
    }

  def collapseMerge(newKeyValue: Memory.Fixed,
                    oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    newKeyValue match {
      case PendingApply(_, newApplies) =>
        //PendingApplies on PendingApplies are never merged. They are just stashed in sequence of their time.
        Memory.PendingApply(newKeyValue.key, oldApplies ++ newApplies)

      case _ =>
        var count = 0
        //reversing so that order is where newer are at the head.
        val reveredApplied = oldApplies.reverse.toList
        reveredApplied.foldLeft(newKeyValue: KeyValue.Fixed) {
          case (newer, older) =>
            count += 1
            //merge as though applies were normal fixed key-values. The result should be the same.
            FixedMerger(newer, older.toMemory(newKeyValue.key)).runRandomIO.right.value match {
              case newPendingApply: KeyValue.PendingApply =>
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

  def assertNotSliced(keyValue: KeyValue): Unit =
    IO(assertSliced(keyValue)).left.runRandomIO.right.value

  def assertSliced(value: Value): Unit =
    value match {
      case Value.Remove(deadline, time) =>
        time.time.shouldBeSliced()

      case Value.Update(value, deadline, time) =>
        value.toOptionC.shouldBeSliced()
        time.time.shouldBeSliced()

      case Value.Function(function, time) =>
        function.shouldBeSliced()
        time.time.shouldBeSliced()

      case Value.PendingApply(applies) =>
        applies foreach assertSliced

      case Value.Put(value, deadline, time) =>
        value.toOptionC.shouldBeSliced()
        time.time.shouldBeSliced()
    }

  def assertSliced(value: Iterable[Value]): Unit =
    value foreach assertSliced

  def assertSliced(keyValue: KeyValue): Unit =
    keyValue match {
      case memory: Memory =>
        memory match {
          case Memory.Put(key, value, deadline, time) =>
            key.shouldBeSliced()
            value.toOptionC.shouldBeSliced()
            time.time.shouldBeSliced()

          case Memory.Update(key, value, deadline, time) =>
            key.shouldBeSliced()
            value.toOptionC.shouldBeSliced()
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
            fromValue foreachS assertSliced
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
            put.getOrFetchValue.runRandomIO.right.value.toOptionC.shouldBeSliced()

          case updated @ Persistent.Update(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            updated.getOrFetchValue.runRandomIO.right.value.toOptionC.shouldBeSliced()

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
            range.fetchFromValueUnsafe.runRandomIO.right.value foreachS assertSliced
            assertSliced(range.fetchRangeValueUnsafe.runRandomIO.right.value)
        }
    }

  def countRangesManually(keyValues: Iterable[Memory]): Int =
    keyValues.foldLeft(0) {
      case (count, keyValue) =>
        keyValue match {
          case fixed: Memory.Fixed =>
            count
          case range: Memory.Range =>
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

  implicit val keyMatcherResultEquality: Equality[KeyMatcher.Result] =
    new Equality[KeyMatcher.Result] {
      override def areEqual(a: KeyMatcher.Result, other: Any): Boolean =
        a match {
          case result: Result.Matched =>
            other match {
              case other: Result.Matched =>
                other.result == result.result

              case _ =>
                false
            }

          case Result.BehindStopped =>
            other == Result.BehindStopped

          case Result.AheadOrNoneOrEnd =>
            other == Result.AheadOrNoneOrEnd

          case Result.BehindFetchNext =>
            other == Result.BehindFetchNext
        }
    }
}
