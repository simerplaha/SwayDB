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
import org.scalatest.{Assertion, Assertions}
import swaydb.core.data._
import swaydb.core.level.zero.{LevelZero, LevelZeroRef}
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.map.MapEntry
import swaydb.core.map.MapEntry.{Add, Remove}
import swaydb.core.map.serializer.MapSerializer
import swaydb.core.segment.Segment
import swaydb.core.segment.format.one.MatchResult.{Matched, Next, Stop}
import swaydb.core.segment.format.one.{KeyMatcher, MatchResult, SegmentReader}
import swaydb.data.slice.{Reader, Slice}
import swaydb.order.KeyOrder

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

trait CommonAssertions extends TryAssert with FutureBase {

  implicit class SliceKeyValueImplicits(actual: Slice[KeyValueType]) {
    def shouldBe(expected: Slice[KeyValueType], ignoreValueOffset: Boolean = false, ignoreStats: Boolean = false): Assertion = {
      actual.size shouldBe expected.size
      var i = 0
      while (i < actual.size) {
        val act = actual(i)
        val exp = expected(i)
        act shouldBe(exp, ignoreValueOffset, ignoreStats)
        i += 1
      }
      Assertions.succeed
    }

    def toMapEntry(implicit serializer: MapSerializer[Slice[Byte], (ValueType, Option[Slice[Byte]])]) =
      actual.foldLeft(Option.empty[MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]]) {
        case (mapEntry, keyValue) =>
          val newEntry: MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])] =
            if (keyValue.isDelete)
              MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](keyValue.key, (ValueType.Remove, keyValue.getOrFetchValue.assertGetOpt))
            else
              MapEntry.Add[Slice[Byte], (ValueType, Option[Slice[Byte]])](keyValue.key, (ValueType.Add, keyValue.getOrFetchValue.assertGetOpt))

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
      Segment.getAllKeyValues(0.1, actual).assertGet shouldBe(Segment.getAllKeyValues(0.1, expected).assertGet, ignoreStats = true)
    }
  }

  implicit class SliceByteImplicits(actual: Slice[Byte]) {
    def shouldBe(expected: Slice[Byte]): Assertion = {
      import KeyOrder.default._
      expected equiv actual shouldBe true
    }

    def shouldHaveSameKey(expected: KeyValueType): Unit = {
      actual shouldBe expected.key
    }
  }

  def getStats(keyValue: KeyValueType): Option[Stats] =
    keyValue match {
      case _: KeyValueReadOnly =>
        None
      case keyValue: KeyValue =>
        Some(keyValue.stats)
    }

  def getValue(keyValue: KeyValueType): Option[Slice[Byte]] =
    keyValue match {
      case keyValue: PersistentType =>
        keyValue.getOrFetchValue.assertGetOpt

      case _ =>
        keyValue.getOrFetchValue.assertGetOpt
    }

  implicit class KeyValueImplicits[KVA <: KeyValueType](actual: KVA) {

    def shouldBeIgnoreStats[KVE <: KeyValueType](expected: KVE): Unit = {
      actual shouldBe(expected, ignoreStats = true)
    }

    def shouldBe[KVE <: KeyValueType](expected: KVE, ignoreValueOffset: Boolean = false, ignoreStats: Boolean = false): Unit = {
      actual.key shouldBe expected.key
      getValue(actual) shouldBe getValue(expected)
      actual.isDelete shouldBe expected.isDelete
      if (!ignoreStats)
        getStats(actual) shouldBe(getStats(expected), ignoreValueOffset)
    }
  }

  implicit class StatsOptionImplicits(actual: Option[Stats]) {
    def shouldBe(expected: Option[Stats], ignoreValueOffset: Boolean = false) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.assertGet shouldBe(expected.assertGet, ignoreValueOffset)
    }
  }

  implicit class ValueTypeValueImplicits(actual: Option[(ValueType, Option[Slice[Byte]])]) {
    def shouldBe(expected: Option[(ValueType, Option[Slice[Byte]])]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }
  }

  implicit class PersistentReadOnlyOptionImplicits(actual: Option[PersistentReadOnly]) {
    def shouldBe(expected: Option[PersistentReadOnly]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }
  }

  implicit class PersistentReadOnlyKeyValueOptionImplicits(actual: Option[PersistentReadOnly]) {
    def shouldBe(expected: Option[KeyValue]) = {
      actual.isDefined shouldBe expected.isDefined
      if (actual.isDefined)
        actual.get shouldBe expected.get
    }

    def shouldBe(expected: KeyValue) =
      actual.assertGet shouldBe expected
  }

  implicit class PersistentReadOnlyKeyValueImplicits(actual: PersistentReadOnly) {
    def shouldBe(expected: KeyValue) = {
      actual.key shouldBe expected.key
      actual.getOrFetchValue.assertGetOpt shouldBe expected.getOrFetchValue.assertGetOpt
      actual.isDelete shouldBe expected.isDelete
    }
  }

  implicit class PersistentReadOnlyImplicits(actual: PersistentReadOnly) {
    def shouldBe(expected: PersistentReadOnly) = {
      actual.key shouldBe expected.key
      actual.getOrFetchValue.assertGetOpt shouldBe expected.getOrFetchValue.assertGetOpt
      actual.isDelete shouldBe expected.isDelete
    }
  }

  implicit class ValueTypeValueImplicits2(actual: (ValueType, Option[Slice[Byte]])) {
    def shouldBe(expected: (ValueType, Option[Slice[Byte]])) = {
      actual._1 shouldBe expected._1
      actual._2 shouldBe expected._2
    }
  }

  implicit class StatsImplicits(actual: Stats) {

    def shouldBe(expected: Stats, ignoreValueOffset: Boolean = false): Assertion = {
      actual.segmentSize shouldBe expected.segmentSize
      actual.valueLength shouldBe expected.valueLength
      if (!ignoreValueOffset && actual.valueLength != 0) {
        actual.valueOffset shouldBe expected.valueOffset
        actual.toValueOffset shouldBe expected.toValueOffset
      }
      actual.segmentSizeWithoutFooter shouldBe expected.segmentSizeWithoutFooter
      actual.segmentValuesSize shouldBe expected.segmentValuesSize
      actual.thisKeyValuesIndexSizeWithoutFooter shouldBe expected.thisKeyValuesIndexSizeWithoutFooter
      actual.thisKeyValuesSegmentSizeWithoutFooter shouldBe expected.thisKeyValuesSegmentSizeWithoutFooter
    }
  }

  implicit class SegmentImplicits(actual: Segment) {

    import KeyOrder.default._

    def shouldBe(expected: Segment): Unit = {
      actual.segmentSize shouldBe expected.segmentSize
      actual.minKey.unslice() equiv expected.minKey.unslice()
      actual.maxKey.unslice() equiv expected.maxKey.unslice()
      actual.existsOnDisk shouldBe expected.existsOnDisk
      actual.path shouldBe expected.path
      //      actual.id shouldBe expected.id
      assertReads(expected.getAll(0.1).get, actual)
    }

    def shouldContainAll(keyValues: Slice[KeyValueType]): Unit =
      keyValues.foreach {
        keyValue =>
          val result = actual.get(keyValue.key).assertGet
          result.key shouldBe keyValue.key
          result.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
          result.isDelete shouldBe keyValue.isDelete
      }
  }

  implicit class MapEntryImplicits[K, V](actual: MapEntry[K, V]) {

    def shouldBe(expected: MapEntry[K, V]): Assertion = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual match {
        case Add(key, value) =>
          expected.isInstanceOf[Add[K, V]] shouldBe true

        case Remove(key) =>
          expected.isInstanceOf[Remove[K, V]] shouldBe true
      }

    }
  }

  implicit class MatchResultImplicits[KV <: KeyValueType](actual: MatchResult[KV]) {
    def shouldBe(expected: MatchResult[KV]): Unit = {
      expected match {
        case Matched(result) =>
          actual match {
            case Matched(actualResult) =>
              actualResult shouldBe result
            case _ =>
              fail(s"Expected ${classOf[Matched[_]].getSimpleName} got $actual")
          }
        case Next =>
          actual match {
            case Next =>
              Assertions.succeed
            case _ =>
              fail(s"Expected ${Next.getClass.getSimpleName} got $actual")
          }

        case Stop =>
          actual match {
            case Stop =>
              Assertions.succeed
            case _ =>
              fail(s"Expected ${Stop.getClass.getSimpleName} got $actual")
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

  implicit class OptionOptionSlice(actual: Option[Option[Slice[Byte]]]) {

    def shouldBe(expected: Option[Slice[Byte]]): Unit = {
      expected match {
        case Some(expected) =>
          actual.isDefined shouldBe true
          actual.get shouldBe Some(expected)
        case None =>
          assert(actual.isEmpty)
      }
    }
  }

  implicit class OptionKeyValue(actual: Option[Slice[Byte]]) {

    import KeyOrder.default._

    def shouldBe(expected: Option[Slice[Byte]]): Unit = {
      expected match {
        case Some(expected) =>
          def printError =
            fail(
              s"""
                 |Expected: $expected
                 |Actual  : $actual
            """.stripMargin)

          if (actual.assertGet.isEmpty)
            printError
          else if (!expected.equiv(actual.assertGet))
            printError

        case None =>
          assert(actual.isEmpty)
      }
    }
  }

  def assertHigher(keyValuesIterable: Iterable[KeyValueType],
                   level: LevelRef) = {
    val keyValues = keyValuesIterable.toArray

    @tailrec
    def assertHigher(index: Int) {
      val lastIndex = keyValues.size - 1
      if (index > keyValues.size - 1) {
        Assertions.succeed
      } else if (index == lastIndex) {
        level.higher(keyValues(lastIndex).key).assertGetOpt shouldBe empty
        assertHigher(index + 1)
      } else {
        val expectedHigherKeyValue = keyValues(index + 1)
        val higher = level.higher(keyValues(index).key).assertGet
        higher.key shouldBe expectedHigherKeyValue.key
        higher.getOrFetchValue.assertGetOpt shouldBe expectedHigherKeyValue.getOrFetchValue.assertGetOpt
        assertHigher(index + 1)
      }
    }

    assertHigher(0)
  }

  def assertLower(keyValuesIterable: Iterable[KeyValueType],
                  level: LevelRef) = {
    val keyValues = keyValuesIterable.toArray

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        Assertions.succeed
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

  def assertLowerFromThisLevel(keyValuesIterable: Iterable[KeyValueType],
                               level: Level) = {
    val keyValues = keyValuesIterable.toArray

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        Assertions.succeed
      } else if (index == 0) {
        level.lowerInThisLevel(keyValues(0).key).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        val lower = level.lowerInThisLevel(keyValues(index).key).assertGet
        lower.key shouldBe expectedLowerKeyValue.key
        lower.getOrFetchValue.assertGetOpt shouldBe expectedLowerKeyValue.getOrFetchValue.assertGetOpt
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValueType],
                   level: LevelZeroRef) = {
    @tailrec
    def assertHigher(index: Int) {
      //      println(s"assertLowers : ${index}")
      val lastIndex = keyValues.size - 1
      if (index > keyValues.size - 1) {
        Assertions.succeed
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

  def assertLower(keyValues: Slice[KeyValueType],
                  level: LevelZeroRef) = {

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        Assertions.succeed
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

  def assertGet(keyValues: Slice[KeyValueType],
                reader: Reader)(implicit ordering: Ordering[Slice[Byte]]) =
    keyValues foreach {
      keyValue =>
        SegmentReader.find(KeyMatcher.Exact(keyValue.key), None, reader.copy()).assertGet shouldBeIgnoreStats keyValue
    }

  def assertBloom(keyValues: Slice[KeyValueType],
                  bloom: BloomFilter[Slice[Byte]])(implicit ordering: Ordering[Slice[Byte]]) =
    keyValues foreach {
      keyValue =>
        bloom.mightContain(keyValue.key) shouldBe true
    }

  def assertReads(keyValues: Slice[KeyValueType],
                  segment: Segment) = {
    val asserts = Seq(() => assertGet(keyValues, segment), () => assertHigher(keyValues, segment), () => assertLower(keyValues, segment))
    Random.shuffle(asserts).foreach(_ ())
  }

  def assertReads(keyValues: Slice[KeyValueType],
                  level: LevelRef) = {
    val asserts = Seq(() => assertGet(keyValues, level), () => assertHigher(keyValues, level), () => assertLower(keyValues, level))
    Random.shuffle(asserts).foreach(_ ())
  }

  def assertGetFromThisLevelOnly(keyValues: Slice[KeyValueType],
                                 level: Level) =
    keyValues foreach {
      keyValue =>
        val actual = level.getFromThisLevel(keyValue.key).assertGet
        actual.isDelete shouldBe keyValue.isDelete
        actual.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertReads(keyValues: Slice[KeyValue],
                  reader: Reader)(implicit ordering: Ordering[Slice[Byte]]) = {

    val footer = SegmentReader.readFooter(reader.copy()).assertGet
    //read fullIndex
    SegmentReader.readAll(footer, reader.copy(), 0.1).assertGet shouldBe keyValues
    //find each KeyValue using all Matchers
    assertGet(keyValues, reader.copy())
    assertLower(keyValues, reader.copy())
    assertHigher(keyValues, reader.copy())
  }

  def assertGet(keyValues: Iterable[KeyValueType],
                segment: Segment) =
    keyValues foreach {
      keyValue =>
        val actual = segment.get(keyValue.key).assertGet
        actual.isDelete shouldBe keyValue.isDelete
        actual.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertGet(keyValues: Iterable[KeyValueType],
                level: LevelRef) =
    keyValues foreach {
      keyValue =>
        val actual = level.get(keyValue.key).assertGet
        actual.isDelete shouldBe keyValue.isDelete
        actual.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertGetNone(keyValues: Iterable[KeyValueType],
                    level: LevelRef) =
    keyValues foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe empty
    }

  def assertGetNone(keyValues: Iterable[KeyValueType],
                    level: LevelZero) =
    keyValues foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe None
    }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValueType],
                                     level: Level) =
    keyValues foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key).assertGetOpt shouldBe empty
    }

  def assertGet(keyValues: Iterable[KeyValueType],
                level: LevelZeroRef) =
    keyValues foreach {
      keyValue =>
        level.get(keyValue.key).assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
    }

  def assertHeadLast(keyValues: Iterable[KeyValueType],
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

  def assertLower(keyValues: Slice[KeyValueType],
                  reader: Reader)(implicit ordering: Ordering[Slice[Byte]]) = {

    @tailrec
    def assertLowers(index: Int) {
      //      println(s"assertLowers : ${index}")
      if (index > keyValues.size - 1) {
        Assertions.succeed
      } else if (index == 0) {
        SegmentReader.find(KeyMatcher.Lower(keyValues(0).key), None, reader.copy()).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLowerKeyValue = keyValues(index - 1)
        SegmentReader.find(KeyMatcher.Lower(keyValues(index).key), None, reader.copy()).assertGet shouldBeIgnoreStats expectedLowerKeyValue
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValueType],
                   reader: Reader)(implicit ordering: Ordering[Slice[Byte]]) = {

    @tailrec
    def assertHigher(index: Int) {
      val lastIndex = keyValues.size - 1
      if (index > keyValues.size - 1) {
        Assertions.succeed
      } else if (index == lastIndex) {
        SegmentReader.find(KeyMatcher.Higher(keyValues(lastIndex).key), None, reader.copy()).assertGetOpt shouldBe empty
        assertHigher(index + 1)
      } else {
        val expectedHigherKeyValue = keyValues(index + 1)
        SegmentReader.find(KeyMatcher.Higher(keyValues(index).key), None, reader.copy()).assertGet shouldBeIgnoreStats expectedHigherKeyValue
        assertHigher(index + 1)
      }
    }

    assertHigher(0)
  }

  def assertLower(keyValues: Slice[KeyValueType],
                  segment: Segment) = {

    @tailrec
    def assertLowers(index: Int) {
      if (index > keyValues.size - 1) {
        Assertions.succeed
      } else if (index == 0) {
        val actualKeyValue = keyValues(index)
        segment.lower(actualKeyValue.key).assertGetOpt shouldBe empty
        assertLowers(index + 1)
      } else {
        val expectedLower = keyValues(index - 1)
        val keyValue = keyValues(index)
        val lower = segment.lower(keyValue.key).assertGet
        lower.key shouldBe expectedLower.key
        lower.getOrFetchValue.assertGetOpt shouldBe expectedLower.getOrFetchValue.assertGetOpt
        lower.isDelete shouldBe expectedLower.isDelete
        assertLowers(index + 1)
      }
    }

    assertLowers(0)
  }

  def assertHigher(keyValues: Slice[KeyValueType],
                   segment: Segment) = {

    (0 until keyValues.size) foreach {
      index =>
        if (index == keyValues.size - 1) {
          val actualKeyValue = keyValues(index)
          segment.higher(actualKeyValue.key).assertGetOpt shouldBe empty
        } else {
          val keyValue = keyValues(index)
          val expectedHigher = keyValues(index + 1)
          val higher = segment.higher(keyValue.key).assertGet
          higher.key shouldBe expectedHigher.key
          higher.getOrFetchValue.assertGetOpt shouldBe expectedHigher.getOrFetchValue.assertGetOpt
          higher.isDelete shouldBe expectedHigher.isDelete
        }
    }
  }

}
