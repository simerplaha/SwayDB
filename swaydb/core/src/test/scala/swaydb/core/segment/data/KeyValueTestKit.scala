package swaydb.core.segment.data

import org.scalatest.matchers.should.Matchers._
import org.scalatest.OptionValues._
import swaydb.core.log.serialiser.LogEntryWriter
import swaydb.core.log.LogEntry
import swaydb.core.segment.{data, CoreFunctionStore, TestCoreFunctionStore}
import swaydb.core.segment.data.Value.{FromValue, FromValueOption, RangeValue}
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.serialiser.{RangeValueSerialiser, ValueSerialiser}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.effect.IOValues._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.{MaxKey, Slice, SliceOption}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.SliceTestKit._
import swaydb.utils.{Aggregator, FiniteDurations}
import swaydb.IO
import swaydb.core.segment.data.Memory.PendingApply
import swaydb.core.segment.data.merge.{FixedMerger, KeyValueMerger}
import swaydb.testkit.TestKit._
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.log.timer.TestTimer

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Random

object KeyValueTestKit {

  implicit class MemoryIterableImplicits(keyValues: Slice[Memory]) {
    @inline final def maxKey(): MaxKey[Slice[Byte]] =
      keyValues.last match {
        case range: Memory.Range =>
          MaxKey.Range(range.fromKey, range.toKey)

        case fixed: Memory.Fixed =>
          MaxKey.Fixed(fixed.key)
      }

    @inline final def minKey: Slice[Byte] =
      keyValues.head.key
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

    def shouldBe(expected: Iterator[KeyValue]): Unit =
      actual shouldBe expected.toList
  }

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

    def shouldBe(expected: KeyValue)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default): Unit = {
      val actualMemory = actual.toMemory()
      val expectedMemory = expected.toMemory()

      actualMemory should be(expectedMemory.cut())
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
              val bytes = Slice.allocate[Byte](ValueSerialiser.bytesRequired(keyValue.getOrFetchApplies.runRandomIO.get))
              ValueSerialiser.write(keyValue.getOrFetchApplies.runRandomIO.get)(bytes)
              Some(bytes)

            case keyValue: Memory.Remove =>
              None

            case keyValue: Memory.Range =>
              val bytes = Slice.allocate[Byte](RangeValueSerialiser.bytesRequired(keyValue.fromValue, keyValue.rangeValue))
              RangeValueSerialiser.write(keyValue.fromValue, keyValue.rangeValue)(bytes)
              Some(bytes)
          }

        case keyValue: Persistent =>
          keyValue match {
            case keyValue: Persistent.Put =>
              keyValue.getOrFetchValue.runRandomIO.get.toOptionC

            case keyValue: Persistent.Update =>
              keyValue.getOrFetchValue.runRandomIO.get.toOptionC

            case keyValue: Persistent.Function =>
              Some(keyValue.getOrFetchFunction.runRandomIO.get)

            case keyValue: Persistent.PendingApply =>
              val applies = keyValue.getOrFetchApplies.runRandomIO.get

              applies.forall(_.isCut) shouldBe true

              val bytes = Slice.allocate[Byte](ValueSerialiser.bytesRequired(applies))
              ValueSerialiser.write(applies)(bytes)
              Some(bytes)

            case keyValue: Persistent.Remove =>
              None

            case range: Persistent.Range =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValueUnsafe.runRandomIO.get
              fromValue.forallS(_.isCut) shouldBe true
              rangeValue.isCut shouldBe true

              val bytes = Slice.allocate[Byte](RangeValueSerialiser.bytesRequired(fromValue, rangeValue))
              RangeValueSerialiser.write(fromValue, rangeValue)(bytes)
              Some(bytes)
          }
      }
  }

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
        case _: Memory.Update    =>
          None

        case _: Memory.Function =>
          None

        case _: Memory.PendingApply =>
          None

        case _: Memory.Remove =>
          None
      }
  }

  implicit class PrintSkipList(skipList: SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]) {

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

  implicit class MemoryImplicits(actual: Iterable[Memory]) {
    def toLogEntry(implicit serialiser: LogEntryWriter[LogEntry.Put[Slice[Byte], Memory]]) =
      actual.foldLeft(Option.empty[LogEntry[Slice[Byte], Memory]]) {
        case (logEntry, keyValue) =>
          val newEntry = LogEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue)
          logEntry.map(_ ++ newEntry) orElse Some(newEntry)
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

  implicit class MergeStatsImplicits(clazz: MergeStats.type) {
    def randomBuilder[FROM](keyValues: Iterable[FROM])(implicit converterOrNull: FROM => data.Memory): MergeStats[FROM, ListBuffer] =
      if (Random.nextBoolean())
        MergeStats.persistentBuilder(keyValues)
      else if (Random.nextBoolean())
        MergeStats.memoryBuilder(keyValues)
      else
        MergeStats.bufferBuilder(keyValues)

    def random(): MergeStats[data.Memory, ListBuffer] =
      if (Random.nextBoolean())
        MergeStats.persistent(Aggregator.listBuffer)(MergeStats.memoryToMemory)
      else if (Random.nextBoolean())
        MergeStats.memory(Aggregator.listBuffer)(MergeStats.memoryToMemory)
      else
        MergeStats.buffer(Aggregator.listBuffer)(MergeStats.memoryToMemory)
  }

  implicit class KeyValueSliceByteImplicits(actual: Slice[Byte]) {
    def shouldHaveSameKey(expected: KeyValue): Unit =
      actual shouldBe expected.key
  }

  implicit class PersistentKeyValueOptionImplicits(actual: PersistentOption) {
    def shouldBe(expected: PersistentOption): Unit = {
      actual.isSomeS shouldBe expected.isSomeS
      if (actual.isSomeS)
        actual.getS shouldBe expected.getS
    }
  }

  implicit class PersistentKeyValueKeyValueOptionImplicits(actual: PersistentOption) {
    def shouldBe(expected: MemoryOption) = {
      actual.isSomeS shouldBe expected.isSomeS
      if (actual.isSomeS)
        actual.getS shouldBe expected.getS
    }

    def shouldBe(expected: Memory): Unit =
      actual.getS shouldBe expected
  }

  implicit class PersistentKeyValueKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: Memory) = {
      actual.toMemory() shouldBe expected.toMemory()
    }
  }

  implicit class PersistentKeyValueImplicits(actual: Persistent) {
    def shouldBe(expected: Persistent) =
      actual.toMemory() shouldBe expected.toMemory()
  }

  def assertNotSliced(keyValue: KeyValue): Unit =
    IO(assertSliced(keyValue)).left.runRandomIO.get

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
          case Persistent.Remove(_key, deadline, _time, indexOffset, nextIndexOffset, nextIndexSize, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()

          case put @ Persistent.Put(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            put.getOrFetchValue.runRandomIO.get.toOptionC.shouldBeSliced()

          case updated @ Persistent.Update(_key, deadline, lazyValueReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            updated.getOrFetchValue.runRandomIO.get.toOptionC.shouldBeSliced()

          case function @ Persistent.Function(_key, lazyFunctionReader, _time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            function.getOrFetchFunction.runRandomIO.get.shouldBeSliced()

          case pendingApply @ Persistent.PendingApply(_key, _time, deadline, lazyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _key.shouldBeSliced()
            _time.time.shouldBeSliced()
            pendingApply.getOrFetchApplies.runRandomIO.get foreach assertSliced

          case range @ Persistent.Range(_fromKey, _toKey, lazyRangeValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, _, _) =>
            _fromKey.shouldBeSliced()
            _toKey.shouldBeSliced()
            range.fetchFromValueUnsafe.runRandomIO.get foreachS assertSliced
            assertSliced(range.fetchRangeValueUnsafe.runRandomIO.get)
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

  implicit class IsExpectedInLastLevel(fromValue: FromValue) {
    def toExpectedLastLevelKeyValue(key: Slice[Byte]): Option[Memory.Fixed] =
      fromValue match {
        case _: Value.Remove =>
          None

        case Value.Put(value, deadline, time) =>
          if (deadline.forall(_.hasTimeLeft()))
            Some(Memory.Put(key, value, deadline, time))
          else
            None

        case _: Value.Update | _: Value.Function | _: Value.PendingApply =>
          None
      }
  }

  implicit class SliceApplyImplicits(applies: Slice[Value.Apply]) {
    def toMemory(key: Slice[Byte]): Slice[Memory.Fixed] =
      applies mapToSlice {
        case Value.Remove(deadline, time) =>
          Memory.Remove(key, deadline, time)

        case Value.Update(value, deadline, time) =>
          Memory.Update(key, value, deadline, time)

        case function: Value.Function =>
          Memory.PendingApply(key, Slice(function))
      }
  }


  def randomDeadUpdateOrExpiredPut(key: Slice[Byte])(implicit functionStore: TestCoreFunctionStore): Memory.Fixed =
    eitherOne(
      randomFixedKeyValue(key, includePuts = false),
      randomPutKeyValue(key, deadline = Some(expiredDeadline()))
    )

  def randomPutKeyValue(key: Slice[Byte],
                        value: SliceOption[Byte] = randomStringSliceOptional(),
                        deadline: Option[Deadline] = randomDeadlineOption())(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Put =
    Memory.Put(
      key = key,
      value = value,
      deadline = deadline,
      time = testTimer.next
    )

  def randomExpiredPutKeyValue(key: Slice[Byte],
                               value: SliceOption[Byte] = randomStringSliceOptional())(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Put =
    randomPutKeyValue(
      key = key,
      value = value,
      deadline = Some(expiredDeadline())
    )

  def randomUpdateKeyValue(key: Slice[Byte],
                           value: SliceOption[Byte] = randomStringSliceOptional(),
                           deadline: Option[Deadline] = randomDeadlineOption())(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Update =
    Memory.Update(key, value, deadline, testTimer.next)

  def randomRemoveKeyValue(key: Slice[Byte],
                           deadline: Option[Deadline] = randomDeadlineOption())(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Remove =
    Memory.Remove(key, deadline, testTimer.next)

  def randomRemoveAny(from: Slice[Byte],
                      to: Slice[Byte],
                      addFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                    functionStore: TestCoreFunctionStore): Memory =
    eitherOne(
      left = randomRemoveOrUpdateOrFunctionRemove(from, addFunctions),
      right = randomRemoveRange(from, to)
    )

  def randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                              functionStore: TestCoreFunctionStore): RangeValue = {
    val value = randomRemoveOrUpdateOrFunctionRemove(Slice.emptyBytes, addFunctions).toRangeValue().runRandomIO.get
    //println(value)
    value
  }

  def randomRemoveFunctionValue()(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                  functionStore: TestCoreFunctionStore): Value.Function =
    randomFunctionKeyValue(Slice.emptyBytes, SegmentFunctionOutput.Remove).toRangeValue().runRandomIO.get

  def randomFunctionValue(output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                  functionStore: TestCoreFunctionStore): Value.Function =
    randomFunctionKeyValue(Slice.emptyBytes, SegmentFunctionOutput.Remove).toRangeValue().runRandomIO.get

  def randomRemoveOrUpdateOrFunctionRemoveValueOption(addFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                    functionStore: TestCoreFunctionStore): Option[RangeValue] =
    eitherOne(
      left = None,
      right = Some(randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions))
    )

  /**
   * Removes can occur by [[Memory.Remove]], [[Memory.Update]] with expiry or [[Memory.Function]] with remove output.
   */
  def randomRemoveOrUpdateOrFunctionRemove(key: Slice[Byte],
                                           addFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                         functionStore: TestCoreFunctionStore): Memory.Fixed =
    if (randomBoolean())
      randomRemoveKeyValue(key, randomExpiredDeadlineOption())
    else if (randomBoolean && addFunctions)
      randomFunctionKeyValue(key, randomRemoveFunctionOutput())
    else
      randomUpdateKeyValue(key, randomStringOption(), Some(expiredDeadline()))

  def randomRemoveFunctionOutput() =
    eitherOne(
      SegmentFunctionOutput.Remove,
      SegmentFunctionOutput.Expire(expiredDeadline()),
      SegmentFunctionOutput.Update(randomStringOption(), Some(expiredDeadline()))
    )

  def randomUpdateFunctionOutput() =
    eitherOne(
      SegmentFunctionOutput.Expire(randomDeadline(false)),
      SegmentFunctionOutput.Update(randomStringOption(), randomDeadlineOption(false))
    )

  def randomRemoveRange(from: Slice[Byte],
                        to: Slice[Byte],
                        addFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                      functionStore: TestCoreFunctionStore): Memory.Range =
    randomRangeKeyValue(
      from = from,
      to = to,
      fromValue = randomRemoveOrUpdateOrFunctionRemoveValueOption(addFunctions) getOrElse Value.FromValue.Null,
      rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions)
    )

  /**
   * Creates remove ranges of random range slices slice for all input key-values.
   */
  def randomRemoveRanges(keyValues: Iterable[Memory])(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                      functionStore: TestCoreFunctionStore): Iterator[Memory.Range] =
    keyValues
      .grouped(randomIntMax(100) max 1)
      .flatMap {
        groupKeyValues =>
          if (groupKeyValues.isEmpty)
            None
          else {
            val maxKeyInt = getMaxKey(groupKeyValues.last).maxKey.readInt()
            assert(groupKeyValues.head.key.readInt() < maxKeyInt + 1)
            Some(
              randomRemoveRange(
                from = groupKeyValues.head.key,
                to = maxKeyInt + 1
              )
            )
          }
      }

  def randomPendingApplyKeyValue(key: Slice[Byte],
                                 max: Int = 5,
                                 value: SliceOption[Byte] = randomStringSliceOptional(),
                                 deadline: Option[Deadline] = randomDeadlineOption(),
                                 functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                                 includeFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                   functionStore: TestCoreFunctionStore) =
    Memory.PendingApply(
      key = key,
      applies =
        randomApplies(
          max = max,
          value = value,
          deadline = deadline,
          functionOutput = functionOutput,
          includeFunctions = includeFunctions
        )
    )

  def createFunction(key: Slice[Byte],
                     coreFunction: SegmentFunction)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                    functionStorage: TestCoreFunctionStore): Memory.Function = {
    val functionId = Slice.writeInt(functionStorage.incrementAndGetId())
    functionStorage.store.put(functionId, coreFunction)
    Memory.Function(key, functionId, testTimer.next)
  }

  def randomFunctionKeyValue(key: Slice[Byte],
                             output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                     functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = randomCoreFunction(output)
    )

  def randomFunctionNoDeadlineKeyValue(key: Slice[Byte],
                                       output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                               functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = randomCoreFunctionNoDeadline(output)
    )

  def randomKeyFunctionKeyValue(key: Slice[Byte],
                                output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                        functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.Key(_ => output)
    )

  def randomKeyDeadlineFunctionKeyValue(key: Slice[Byte],
                                        output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                                functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.KeyDeadline((_, _) => output)
    )

  def randomKeyValueFunctionKeyValue(key: Slice[Byte],
                                     output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                             functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.KeyValue((_, _) => output)
    )

  def randomKeyValueDeadlineFunctionKeyValue(key: Slice[Byte],
                                             output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                                     functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.KeyValueDeadline((_, _, _) => output)
    )

  def randomValueFunctionKeyValue(key: Slice[Byte],
                                  output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                          functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.Value(_ => output)
    )

  def randomValueDeadlineFunctionKeyValue(key: Slice[Byte],
                                          output: SegmentFunctionOutput = randomFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                                  functionStorage: TestCoreFunctionStore): Memory.Function =
    createFunction(
      key = key,
      coreFunction = SegmentFunction.ValueDeadline((_, _) => output)
    )

  def randomFunctionOutput(addRemoves: Boolean = randomBoolean(), expiredDeadline: Boolean = randomBoolean()): SegmentFunctionOutput =
    if (addRemoves && randomBoolean())
      SegmentFunctionOutput.Remove
    else if (randomBoolean())
      SegmentFunctionOutput.Nothing
    else
      randomFunctionUpdateOutput(expiredDeadline)

  def randomFunctionUpdateOutput(expiredDeadline: Boolean = randomBoolean()): SegmentFunctionOutput =
    if (randomBoolean())
      SegmentFunctionOutput.Expire(randomDeadline(expiredDeadline))
    else
      SegmentFunctionOutput.Update((randomStringOption(): Slice[Byte]).asSliceOption(), randomDeadlineOption(expiredDeadline))

  def randomRequiresKeyFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction.RequiresKey =
    Random.shuffle(
      Seq[SegmentFunction.RequiresKey](
        SegmentFunction.Key(_ => functionOutput),
        SegmentFunction.KeyValue((_, _) => functionOutput),
        SegmentFunction.KeyDeadline((_, _) => functionOutput),
        SegmentFunction.KeyValueDeadline((_, _, _) => functionOutput)
      )
    ).head

  def randomRequiresKeyOnlyWithOptionDeadlineFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction.RequiresKey =
    Random.shuffle(
      Seq[SegmentFunction.RequiresKey](
        SegmentFunction.Key(_ => functionOutput),
        SegmentFunction.KeyDeadline((_, _) => functionOutput)
      )
    ).head

  def randomValueOnlyFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction.RequiresValue =
    Random.shuffle(
      Seq[SegmentFunction.RequiresValue](
        SegmentFunction.Value(_ => functionOutput),
        SegmentFunction.ValueDeadline((_, _) => functionOutput)
      )
    ).head

  def randomRequiresValueWithOptionalKeyAndDeadlineFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction.RequiresValue =
    Random.shuffle(
      Seq[SegmentFunction.RequiresValue](
        SegmentFunction.Value(_ => functionOutput),
        SegmentFunction.KeyValueDeadline((_, _, _) => functionOutput),
        SegmentFunction.KeyValue((_, _) => functionOutput),
        SegmentFunction.ValueDeadline((_, _) => functionOutput)
      )
    ).head

  def randomCoreFunctionNoDeadline(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction =
    Random.shuffle(
      Seq(
        SegmentFunction.Value(_ => functionOutput),
        SegmentFunction.Key(_ => functionOutput),
        SegmentFunction.KeyValue((_, _) => functionOutput)
      )
    ).head

  def randomRequiresDeadlineFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction.RequiresDeadline =
    Random.shuffle(
      Seq[SegmentFunction.RequiresDeadline](
        SegmentFunction.KeyDeadline((_, _) => functionOutput),
        SegmentFunction.KeyValueDeadline((_, _, _) => functionOutput),
        SegmentFunction.ValueDeadline((_, _) => functionOutput)
      )
    ).head


  implicit class FunctionOutputImplicits(functionOutput: SegmentFunctionOutput) {
    def toMemory(key: Slice[Byte],
                 time: Time): Memory.Fixed =
      functionOutput match {
        case SegmentFunctionOutput.Remove =>
          Memory.Remove(key, None, time)

        case SegmentFunctionOutput.Expire(deadline) =>
          Memory.Remove(key, Some(deadline), time)

        case SegmentFunctionOutput.Update(newValue, newDeadline) =>
          Memory.Update(key, newValue, newDeadline, time)

        case SegmentFunctionOutput.Nothing =>
          fail("coreFunctionOutput.Nothing")
      }
  }

  def randomCoreFunction(functionOutput: SegmentFunctionOutput = randomFunctionOutput()): SegmentFunction =
    if (randomBoolean())
      randomRequiresKeyFunction(functionOutput)
    else
      randomValueOnlyFunction(functionOutput)

  def randomFunctionId(functionOutput: SegmentFunctionOutput = randomFunctionOutput())(implicit functionStore: TestCoreFunctionStore): Slice[Byte] = {
    val functionId = Slice.writeInt(functionStore.incrementAndGetId())
    functionStore.store.put(functionId, randomCoreFunction(functionOutput))
    functionId
  }

  def randomApply(value: SliceOption[Byte] = randomStringSliceOptional(),
                  deadline: Option[Deadline] = randomDeadlineOption(),
                  addRemoves: Boolean = randomBoolean(),
                  functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                  includeFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                    functionStore: TestCoreFunctionStore) =
    if (addRemoves && randomBoolean())
      Value.Remove(deadline, testTimer.next)
    else if (includeFunctions && randomBoolean())
      Value.Function(randomFunctionId(functionOutput), testTimer.next)
    else
      Value.Update(value, deadline, testTimer.next)

  def randomApplyWithDeadline(value: SliceOption[Byte] = randomStringSliceOptional(),
                              addRangeRemoves: Boolean = randomBoolean(),
                              deadline: Deadline = randomDeadline())(implicit testTimer: TestTimer = TestTimer.Incremental()) =
    if (addRangeRemoves && randomBoolean())
      Value.Remove(Some(deadline), testTimer.next)
    else
      Value.Update(value, Some(deadline), testTimer.next)

  def randomApplies(max: Int = 5,
                    value: SliceOption[Byte] = randomStringSliceOptional(),
                    deadline: Option[Deadline] = randomDeadlineOption(),
                    addRemoves: Boolean = randomBoolean(),
                    functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                    includeFunctions: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                      functionStore: TestCoreFunctionStore): Slice[Value.Apply] =
    Slice.wrap {
      (1 to (Random.nextInt(max) max 1)).map {
        _ =>
          randomApply(
            value = value,
            deadline = deadline,
            addRemoves = addRemoves,
            functionOutput = functionOutput,
            includeFunctions = includeFunctions
          )
      } toArray
    }

  def randomAppliesWithDeadline(max: Int = 5,
                                value: SliceOption[Byte] = randomStringSliceOptional(),
                                addRangeRemoves: Boolean = randomBoolean(),
                                deadline: Deadline = randomDeadline())(implicit testTimer: TestTimer = TestTimer.Incremental()): Slice[Value.Apply] =
    Slice.wrap {
      (1 to (Random.nextInt(max) max 1)).map {
        _ =>
          randomApplyWithDeadline(
            value = value,
            addRangeRemoves = addRangeRemoves,
            deadline = deadline
          )
      } toArray
    }

  def randomTransientKeyValue(key: Slice[Byte],
                              toKey: SliceOption[Byte],
                              value: SliceOption[Byte] = randomStringSliceOptional(),
                              rangeValue: RangeValue,
                              deadline: Option[Deadline] = randomDeadlineOption(),
                              time: Time = Time.empty,
                              functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                              includePendingApply: Boolean = true,
                              includeFunctions: Boolean = true,
                              includeRemoves: Boolean = true,
                              includePuts: Boolean = true,
                              includeRanges: Boolean = true)(implicit functionStore: TestCoreFunctionStore): Memory =
    if (toKey.isSomeC && includeRanges && randomBoolean())
      randomRangeKeyValue(
        from = key,
        to = toKey.getC,
        fromValue = randomFromValueOption(),
        rangeValue = rangeValue
      )
    else
      randomFixedTransientKeyValue(
        key = key,
        value = value,
        deadline = deadline,
        time = time,
        functionOutput = functionOutput,
        includePendingApply = includePendingApply,
        includeFunctions = includeFunctions,
        includeRemoves = includeRemoves,
        includePuts = includePuts
      )

  def randomFixedTransientKeyValue(key: Slice[Byte],
                                   value: SliceOption[Byte] = randomStringSliceOptional(),
                                   deadline: Option[Deadline] = randomDeadlineOption(),
                                   time: Time = Time.empty,
                                   functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                                   includePendingApply: Boolean = true,
                                   includeFunctions: Boolean = true,
                                   includeRemoves: Boolean = true,
                                   includePuts: Boolean = true)(implicit functionStore: TestCoreFunctionStore): Memory.Fixed =
    if (includePuts && randomBoolean())
      Memory.Put(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )
    else if (includeRemoves && randomBoolean())
      Memory.Remove(
        key = key,
        deadline = deadline,
        time = time
      )
    else if (includeFunctions && randomBoolean())
      Memory.Function(
        key = key,
        function = randomFunctionId(functionOutput),
        time = time
      )
    else if (includePendingApply && randomBoolean())
      Memory.PendingApply(
        key = key,
        applies =
          randomApplies(
            max = 10,
            value = value,
            deadline = deadline,
            addRemoves = includeRemoves,
            functionOutput = functionOutput,
            includeFunctions = includeFunctions
          )
      )
    else
      Memory.Update(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )

  def randomFixedKeyValue(key: Slice[Byte],
                          value: SliceOption[Byte] = randomStringSliceOptional(),
                          deadline: Option[Deadline] = randomDeadlineOption(),
                          functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                          includePendingApply: Boolean = true,
                          includeFunctions: Boolean = true,
                          includeRemoves: Boolean = true,
                          includePuts: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                       functionStore: TestCoreFunctionStore): Memory.Fixed =
    if (includePuts && randomBoolean())
      Memory.Put(key, value, deadline, testTimer.next)
    else if (includeRemoves && randomBoolean())
      Memory.Remove(key, deadline, testTimer.next)
    else if (includeFunctions && randomBoolean())
      Memory.Function(key, randomFunctionId(functionOutput), testTimer.next)
    else if (includePendingApply && randomBoolean())
      Memory.PendingApply(
        key = key,
        applies =
          randomApplies(
            max = 10,
            value = value,
            deadline = deadline,
            addRemoves = includeRemoves,
            functionOutput = functionOutput,
            includeFunctions = includeFunctions
          )
      )
    else
      Memory.Update(key, value, deadline, testTimer.next)

  def randomRangeKeyValue(from: Slice[Byte],
                          to: Slice[Byte],
                          fromValue: Value.FromValueOption,
                          rangeValue: Value.RangeValue): Memory.Range =
    Memory.Range(
      fromKey = from,
      toKey = to,
      fromValue = fromValue,
      rangeValue = rangeValue
    )

  def randomRangeKeyValue(from: Slice[Byte],
                          to: Slice[Byte]): Memory.Range = {
    implicit val timer: TestTimer = TestTimer.random
    implicit val functionStore: TestCoreFunctionStore = TestCoreFunctionStore()

    val range = Memory.Range(from, to, randomFromValueOption(), randomRangeValue())
    //println(range)
    range
  }

  def randomRangeKeyValueWithDeadline(from: Slice[Byte],
                                      to: Slice[Byte],
                                      fromValue: FromValueOption = randomFromValueWithDeadlineOption()(TestTimer.random),
                                      rangeValue: RangeValue = randomRangeValueWithDeadline()(TestTimer.random)): Memory.Range = {
    val range = Memory.Range(from, to, fromValue, rangeValue)
    //println(range)
    range
  }

  def randomRangeKeyValueWithFromValueExpiredDeadline(from: Slice[Byte],
                                                      to: Slice[Byte],
                                                      fromValue: FromValueOption = randomFromValueWithDeadlineOption(deadline = expiredDeadline())(TestTimer.random),
                                                      rangeValue: RangeValue = randomRangeValueWithDeadline()(TestTimer.random)): Memory.Range =
    randomRangeKeyValueWithDeadline(from, to, fromValue, rangeValue)

  def randomRangeKeyValueForDeadline(from: Slice[Byte],
                                     to: Slice[Byte],
                                     deadline: Deadline = randomDeadline()): Memory.Range =
    Memory.Range(
      fromKey = from,
      toKey = to,
      fromValue = randomFromValueWithDeadlineOption(deadline = deadline)(TestTimer.random),
      rangeValue = randomRangeValueWithDeadline(deadline = deadline)(TestTimer.random)
    )

  def randomRangeValueOption(from: Slice[Byte], to: Slice[Byte]): Option[Memory.Range] =
    if (randomBoolean())
      Some(randomRangeKeyValue(from, to))
    else
      None

  def randomFromValueOption(value: SliceOption[Byte] = randomStringSliceOptional(),
                            deadline: Option[Deadline] = randomDeadlineOption(),
                            functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                            addRemoves: Boolean = randomBoolean(),
                            addPut: Boolean = randomBoolean())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                               functionStore: TestCoreFunctionStore): FromValueOption =
    if (randomBoolean())
      randomFromValue(
        value = value,
        addRemoves = addRemoves,
        functionOutput = functionOutput,
        deadline = deadline,
        addPut = addPut
      )
    else
      Value.FromValue.Null

  def randomFromValueWithDeadlineOption(value: SliceOption[Byte] = randomStringSliceOptional(),
                                        addRangeRemoves: Boolean = randomBoolean(),
                                        deadline: Deadline = randomDeadline())(implicit testTimer: TestTimer = TestTimer.Incremental()): FromValueOption =
    if (randomBoolean())
      randomFromValueWithDeadline(value, addRangeRemoves, deadline)
    else
      Value.FromValue.Null

  def randomUpdateRangeValue(value: SliceOption[Byte] = randomStringSliceOptional(),
                             addRemoves: Boolean = randomBoolean(),
                             functionOutput: SegmentFunctionOutput = randomUpdateFunctionOutput())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                                                   functionStore: TestCoreFunctionStore) = {
    val deadline =
    //if removes are allowed make sure to set the deadline
      if (addRemoves)
        Some(randomDeadline(false))
      else
        randomDeadlineOption(false)

    randomRangeValue(value = value, addRemoves = addRemoves, functionOutput = functionOutput, deadline = deadline)
  }

  def randomFromValue(value: SliceOption[Byte] = randomStringSliceOptional(),
                      addRemoves: Boolean = randomBoolean(),
                      deadline: Option[Deadline] = randomDeadlineOption(),
                      functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                      addPut: Boolean = randomBoolean())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                         functionStore: TestCoreFunctionStore): Value.FromValue =
    if (addPut && randomBoolean())
      Value.Put(value, deadline, testTimer.next)
    else
      randomRangeValue(value = value, addRemoves = addRemoves, functionOutput = functionOutput, deadline = deadline)

  def randomRangeValue(value: SliceOption[Byte] = randomStringSliceOptional(),
                       deadline: Option[Deadline] = randomDeadlineOption(),
                       functionOutput: SegmentFunctionOutput = randomFunctionOutput(),
                       addRemoves: Boolean = randomBoolean())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                              functionStore: TestCoreFunctionStore): Value.RangeValue =
    if (addRemoves && randomBoolean())
      Value.Remove(deadline, testTimer.next)
    else if (randomBoolean())
      Value.Function(randomFunctionId(functionOutput), testTimer.next)
    else if (randomBoolean())
      Value.PendingApply(randomApplies(value = value, addRemoves = addRemoves, deadline = deadline, functionOutput = functionOutput))
    else
      Value.Update(value, deadline, testTimer.next)

  def randomFromValueWithDeadline(value: SliceOption[Byte] = randomStringSliceOptional(),
                                  addRangeRemoves: Boolean = randomBoolean(),
                                  deadline: Deadline = randomDeadline())(implicit testTimer: TestTimer = TestTimer.Incremental()): Value.FromValue =
    if (randomBoolean())
      Value.Put(value, Some(deadline), testTimer.next)
    else
      randomRangeValueWithDeadline(value = value, addRangeRemoves = addRangeRemoves, deadline = deadline)

  def randomRangeValueWithDeadline(value: SliceOption[Byte] = randomStringSliceOptional(),
                                   addRangeRemoves: Boolean = randomBoolean(),
                                   deadline: Deadline = randomDeadline())(implicit testTimer: TestTimer = TestTimer.Incremental()): Value.RangeValue =
    if (addRangeRemoves && randomBoolean())
      Value.Remove(Some(deadline), testTimer.next)
    else if (randomBoolean())
      Value.PendingApply(randomAppliesWithDeadline(value = value, deadline = deadline))
    else
      Value.Update(value, Some(deadline), testTimer.next)


  def randomIntKeyStringValues(count: Int = 5,
                               startId: Option[Int] = None,
                               valueSize: Int = 50,
                               addRemoves: Boolean = false,
                               addRanges: Boolean = false,
                               addRemoveDeadlines: Boolean = false,
                               addPutDeadlines: Boolean = false)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                 functionStore: TestCoreFunctionStore): Slice[Memory] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addRemoves = addRemoves,
      addRanges = addRanges,
      addRemoveDeadlines = addRemoveDeadlines,
      addPutDeadlines = addPutDeadlines
    )

  def randomizedKeyValues(count: Int = 5,
                          startId: Option[Int] = None,
                          valueSize: Int = 50,
                          addPut: Boolean = true,
                          addRemoves: Boolean = randomBoolean(),
                          addRangeRemoves: Boolean = randomBoolean(),
                          addUpdates: Boolean = randomBoolean(),
                          addFunctions: Boolean = randomBoolean(),
                          addRanges: Boolean = randomBoolean(),
                          addPendingApply: Boolean = randomBoolean(),
                          addRemoveDeadlines: Boolean = randomBoolean(),
                          addPutDeadlines: Boolean = randomBoolean(),
                          addExpiredPutDeadlines: Boolean = randomBoolean(),
                          addUpdateDeadlines: Boolean = randomBoolean())(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                                         functionStore: TestCoreFunctionStore): Slice[Memory] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addPut = addPut,
      addRemoves = addRemoves,
      addRangeRemoves = addRangeRemoves,
      addUpdates = addUpdates,
      addFunctions = addFunctions,
      addRanges = addRanges,
      addPendingApply = addPendingApply,
      addRemoveDeadlines = addRemoveDeadlines,
      addPutDeadlines = addPutDeadlines,
      addExpiredPutDeadlines = addExpiredPutDeadlines,
      addUpdateDeadlines = addUpdateDeadlines
    )

  def randomPutKeyValues(count: Int = 5,
                         startId: Option[Int] = None,
                         valueSize: Int = 50,
                         addRemoves: Boolean = false,
                         addRanges: Boolean = false,
                         addRemoveDeadlines: Boolean = false,
                         addPutDeadlines: Boolean = true,
                         addExpiredPutDeadlines: Boolean = false)(implicit testTimer: TestTimer = TestTimer.random,
                                                                  functionStore: TestCoreFunctionStore): Slice[Memory] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addPut = true,
      addRemoves = addRemoves,
      addRanges = addRanges,
      addExpiredPutDeadlines = addExpiredPutDeadlines,
      addRemoveDeadlines = addRemoveDeadlines,
      addPutDeadlines = addPutDeadlines
    )

  def randomKeyValues(count: Int = 20,
                      startId: Option[Int] = None,
                      valueSize: Int = 50,
                      addPut: Boolean = true,
                      addRemoves: Boolean = false,
                      addRangeRemoves: Boolean = false,
                      addUpdates: Boolean = false,
                      addFunctions: Boolean = false,
                      addRemoveDeadlines: Boolean = false,
                      addPendingApply: Boolean = false,
                      addPutDeadlines: Boolean = false,
                      addExpiredPutDeadlines: Boolean = false,
                      addUpdateDeadlines: Boolean = false,
                      addRanges: Boolean = false)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                  functionStore: TestCoreFunctionStore): Slice[Memory] = {
    val slice = Slice.allocate[Memory](count * 50) //extra space because addRanges and random Groups can be added for Fixed and Range key-values in the same iteration.
    //            var key = 1
    var key = startId getOrElse randomInt(minus = count)
    var iteration = 0
    while (slice.size < count) {
      iteration += 1
      //      if (slice.written % 100000 == 0) println(s"Generated ${slice.written} key-values.")
      //protect from going into infinite loop
      if ((iteration >= count * 5) && slice.isEmpty) fail(s"Too many iterations ($iteration) without generated key-values. Expected $count.")

      if (addRanges && randomBoolean()) {
        val toKey = key + 10
        val fromValueValueBytes = eitherOne(Slice.Null, randomBytesSlice(valueSize))
        val rangeValueValueBytes = eitherOne(Slice.Null, randomBytesSlice(valueSize))
        val fromValueDeadline =
          if (addPutDeadlines || addRemoveDeadlines || addUpdateDeadlines)
            randomDeadlineOption(addExpiredPutDeadlines)
          else
            None
        val rangeValueDeadline = if (addRemoveDeadlines || addUpdateDeadlines) randomDeadlineOption() else None
        slice add
          randomRangeKeyValue(
            from = key,
            to = toKey,
            fromValue = randomFromValueOption(value = fromValueValueBytes, deadline = fromValueDeadline, addPut = addPut),
            rangeValue = randomRangeValue(value = rangeValueValueBytes, addRemoves = addRangeRemoves, deadline = rangeValueDeadline)
          )
        //randomly skip the Range's toKey for the next key.
        if (randomBoolean())
          key = toKey
        else
          key = toKey + randomIntMax(5)
      } else if (addRemoves && randomBoolean()) {
        slice add
          randomRemoveKeyValue(
            key = key: Slice[Byte],
            deadline = if (addRemoveDeadlines) randomDeadlineOption() else None
          )
        key = key + 1
      } else if (addUpdates && randomBoolean()) {
        val valueBytes = if (valueSize == 0) Slice.Null else eitherOne(Slice.Null, randomBytesSlice(valueSize))
        slice add
          randomUpdateKeyValue(
            key = key: Slice[Byte],
            deadline = if (addUpdateDeadlines) randomDeadlineOption() else None,
            value = valueBytes
          )
        key = key + 1
      } else if (addFunctions && randomBoolean()) {
        slice add
          randomFunctionKeyValue(
            key = key: Slice[Byte]
          )
        key = key + 1
      } else if (addPendingApply && randomBoolean()) {
        val valueBytes = if (valueSize == 0) Slice.Null else eitherOne(Slice.Null, randomBytesSlice(valueSize))
        slice add
          randomPendingApplyKeyValue(
            key = key: Slice[Byte],
            deadline = if (addUpdateDeadlines) randomDeadlineOption() else None,
            value = valueBytes
          )
        key = key + 1
      } else if (addPut) {
        val valueBytes = if (valueSize == 0) Slice.Null else eitherOne(Slice.Null, randomBytesSlice(valueSize))
        val deadline = if (addPutDeadlines) randomDeadlineOption(addExpiredPutDeadlines) else None
        slice add
          randomPutKeyValue(
            key = key: Slice[Byte],
            deadline = deadline,
            value = valueBytes
          )
        key = key + 1
      } else {
        key = key + 1
      }
    }
    //    println(s"Generated: ${slice.size} over iterations: $iteration")
    slice.close()
  }

  def randomFixedNoneValue(count: Int = 20,
                           startId: Option[Int] = None,
                           addUpdates: Boolean = true,
                           addUpdateDeadlines: Boolean = true,
                           addPutDeadlines: Boolean = true,
                           addRemoves: Boolean = true,
                           addRemoveDeadlines: Boolean = true)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                               functionStore: TestCoreFunctionStore): Slice[Memory] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = 0,
      addUpdates = addUpdates,
      addUpdateDeadlines = addUpdateDeadlines,
      addPutDeadlines = addPutDeadlines,
      addRemoves = addRemoves,
      addRemoveDeadlines = addRemoveDeadlines)

  implicit class MemoryTypeImplicits(memory: Memory.type) {

    def put(key: Slice[Byte],
            value: Slice[Byte])(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, None, testTimer.next)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAt: Deadline)(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, Some(removeAt), testTimer.next)

    def put(key: Slice[Byte],
            value: SliceOption[Byte],
            removeAt: Deadline)(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, Some(removeAt), testTimer.next)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAt: Option[Deadline])(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, removeAt, testTimer.next)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAfter: FiniteDuration)(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, Some(removeAfter.fromNow), testTimer.next)

    def put(key: Slice[Byte],
            value: SliceOption[Byte])(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, value, None, testTimer.next)

    def put(key: Slice[Byte])(implicit testTimer: TestTimer): Memory.Put =
      Memory.Put(key, Slice.Null, None, testTimer.next)

    def put(key: Slice[Byte],
            value: SliceOption[Byte],
            deadline: Option[Deadline],
            time: Time): Memory.Put =
      Memory.Put(key, value, deadline, time)

    def put(key: Slice[Byte],
            value: SliceOption[Byte],
            deadline: Option[Deadline])(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Put =
      Memory.Put(key, value, deadline, testTimer.next)

    def update(key: Slice[Byte],
               value: Slice[Byte])(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, None, testTimer.next)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAt: Deadline)(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, Some(removeAt), testTimer.next)

    def update(key: Slice[Byte],
               value: SliceOption[Byte],
               removeAt: Deadline)(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, Some(removeAt), testTimer.next)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAt: Option[Deadline])(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, removeAt, testTimer.next)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAfter: FiniteDuration)(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, Some(removeAfter.fromNow), testTimer.next)

    def update(key: Slice[Byte],
               value: SliceOption[Byte])(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, value, None, testTimer.next)

    def update(key: Slice[Byte])(implicit testTimer: TestTimer): Memory.Update =
      Memory.Update(key, Slice.Null, None, testTimer.next)

    def update(key: Slice[Byte],
               value: SliceOption[Byte],
               deadline: Option[Deadline])(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Update =
      Memory.Update(key, value, deadline, testTimer.next)

    def remove(key: Slice[Byte]): Memory.Remove =
      Memory.Remove(key, None, Time.empty)

    def remove(key: Slice[Byte], deadline: Deadline): Memory.Remove =
      Memory.Remove(key, Some(deadline), Time.empty)

    def remove(key: Slice[Byte], deadline: FiniteDuration): Memory.Remove =
      Memory.Remove(key, Some(deadline.fromNow), Time.empty)

    def remove(key: Slice[Byte],
               deadline: Option[Deadline])(implicit testTimer: TestTimer = TestTimer.Incremental()): Memory.Remove =
      Memory.Remove(key, deadline, testTimer.next)
  }

  implicit class ValueUpdateTypeImplicits(remove: Value.type) {

    def remove(deadline: Option[Deadline],
               time: Time): Value.Remove =
      Value.Remove(deadline, time)

    def remove(deadline: Deadline)(implicit testTimer: TestTimer): Value.Remove =
      Value.Remove(Some(deadline), testTimer.next)

    def remove(deadline: Option[Deadline])(implicit testTimer: TestTimer): Value.Remove =
      Value.Remove(deadline, testTimer.next)

    def put(value: SliceOption[Byte],
            deadline: Option[Deadline],
            time: Time)(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, deadline, time)

    def put(value: Slice[Byte])(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, None, testTimer.next)

    def put(value: SliceOption[Byte])(removeAfter: Deadline)(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, Some(removeAfter), testTimer.next)

    def put(value: Slice[Byte], removeAfter: Deadline)(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, Some(removeAfter), testTimer.next)

    def put(value: SliceOption[Byte], removeAfter: Option[Deadline])(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, removeAfter, testTimer.next)

    def put(value: Slice[Byte], duration: FiniteDuration)(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, Some(duration.fromNow), testTimer.next)

    def put(value: SliceOption[Byte], duration: FiniteDuration)(implicit testTimer: TestTimer): Value.Put =
      Value.Put(value, Some(duration.fromNow), testTimer.next)

    def update(value: SliceOption[Byte],
               deadline: Option[Deadline],
               time: Time): Value.Update =
      Value.Update(value, deadline, time)

    def update(value: Slice[Byte])(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, None, testTimer.next)

    def update(value: Slice[Byte], deadline: Option[Deadline])(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, deadline, testTimer.next)

    def update(value: SliceOption[Byte])(removeAfter: Deadline)(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, Some(removeAfter), testTimer.next)

    def update(value: Slice[Byte], removeAfter: Deadline)(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, Some(removeAfter), testTimer.next)

    def update(value: SliceOption[Byte], removeAfter: Option[Deadline])(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, removeAfter, testTimer.next)

    def update(value: Slice[Byte], duration: FiniteDuration)(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, Some(duration.fromNow), testTimer.next)

    def update(value: SliceOption[Byte], duration: FiniteDuration)(implicit testTimer: TestTimer): Value.Update =
      Value.Update(value, Some(duration.fromNow), testTimer.next)
  }

  def collectUsedDeadlines(keyValues: Slice[Memory], usedDeadlines: List[Deadline]): List[Deadline] =
    keyValues.foldLeft(usedDeadlines) {
      case (usedDeadlines, keyValue) =>
        keyValue match {
          case remove: Memory.Remove =>
            usedDeadlines ++ remove.deadline

          case put: Memory.Put =>
            usedDeadlines ++ put.deadline

          case update: Memory.Update =>
            usedDeadlines ++ update.deadline

          case _: Memory.Function =>
            usedDeadlines

          case apply: Memory.PendingApply =>
            collectUsedDeadlines(apply.applies.mapToSlice(_.toMemory(Slice.emptyBytes)), usedDeadlines)

          case range: Memory.Range =>
            val fromTransient = range.fromValue.toOptionS.map(_.toMemory(Slice.emptyBytes))
            val rangeTransient = range.rangeValue.toMemory(Slice.emptyBytes)
            collectUsedDeadlines(Slice(rangeTransient) ++ fromTransient, usedDeadlines)
        }
    }

  def collectUsedPutDeadlines(keyValues: Slice[Memory], usedDeadlines: List[Deadline]): Slice[Deadline] =
    keyValues collectToSlice {
      case put: Memory.Put if put.deadline.isDefined =>
        put.deadline.get

      case range: Memory.Range if range.fromValue.existsS(fromValue => fromValue.isPut && fromValue.deadline.isDefined) =>
        range.fromValue.getS.deadline.get
    }

  def nearestDeadline(keyValues: Slice[Memory]): Option[Deadline] = {
    val usedDeadlines = collectUsedDeadlines(keyValues.toSlice, List.empty)
    if (usedDeadlines.isEmpty)
      None
    else
      Some(
        usedDeadlines.reduce[Deadline] {
          case (left, right) =>
            if (left < right)
              left
            else
              right
        }
      )
  }

  def nearestPutDeadline(keyValues: Slice[Memory]): Option[Deadline] = {
    val usedDeadlines = collectUsedPutDeadlines(keyValues.toSlice, List.empty)
    if (usedDeadlines.isEmpty)
      None
    else
      Some(
        usedDeadlines.reduce[Deadline] {
          case (left, right) =>
            if (left < right)
              left
            else
              right
        }
      )
  }

  def maxKey(keyValues: Slice[Memory]): MaxKey[Slice[Byte]] =
    getMaxKey(keyValues.last)

  def getMaxKey(transient: Memory): MaxKey[Slice[Byte]] =
    transient match {
      case last: Memory.Fixed =>
        MaxKey.Fixed(last.key)

      case last: Memory.Range =>
        MaxKey.Range(last.fromKey, last.toKey)
    }

  def unexpiredPuts(keyValues: IterableOnce[KeyValue]): Slice[KeyValue.Put] = {
    val slice = Slice.allocate[KeyValue.Put](keyValues.size)
    keyValues foreach {
      keyValue =>
        keyValue.asPut foreach {
          put =>
            if (put.hasTimeLeft())
              slice add put
        }
    }
    slice
  }

  def furthestDeadline(keyValues: IterableOnce[KeyValue]): Option[Deadline] =
    keyValues.foldLeft(Option.empty[Deadline]) {
      case (furthestDeadline, keyValue) =>
        keyValue.asPut match {
          case Some(put) =>
            FiniteDurations.getFurthestDeadline(put.deadline, furthestDeadline)

          case None =>
            furthestDeadline
        }
    }

  def getPuts(keyValues: Iterable[KeyValue]): Iterable[KeyValue.Put] =
    keyValues collect {
      case put: KeyValue.Put =>
        put

      case range: KeyValue.Range if range.fetchFromValueUnsafe.isSomeS && range.fetchFromValueUnsafe.getS.isPut =>
        range.fetchFromValueUnsafe.getS.toPutMayBe(range.key).value
    }

  def getPutsWithDeadline(keyValues: Iterable[KeyValue]): Iterable[KeyValue.Put] =
    keyValues collect {
      case put: KeyValue.Put if put.deadline.isDefined =>
        put

      case range: KeyValue.Range if range.fetchFromValueUnsafe.isSomeS && range.fetchFromValueUnsafe.getS.isPut && range.fetchFromValueUnsafe.getS.deadline.isDefined =>
        range.fetchFromValueUnsafe.getS.toPutMayBe(range.key).value
    }

  def getRanges(keyValues: Iterable[KeyValue]): Iterable[KeyValue.Range] =
    keyValues collect {
      case range: KeyValue.Range => range
    }

  def getUpdates(keyValues: Iterable[KeyValue]): Iterable[KeyValue.Fixed] =
    keyValues collect {
      case keyValue: KeyValue.Update       => keyValue
      case keyValue: KeyValue.Remove       => keyValue
      case keyValue: KeyValue.PendingApply => keyValue
      case keyValue: KeyValue.Function     => keyValue
    }

  /**
   * Randomly updates all key-values using one of the many update methods.
   *
   * Used for testing all updates work for all existing put key-values.
   */
  def randomUpdate(keyValues: Iterable[KeyValue.Put],
                   updatedValue: SliceOption[Byte],
                   deadline: Option[Deadline],
                   randomlyDropUpdates: Boolean)(implicit testTimer: TestTimer = TestTimer.Incremental(),
                                                 functionStore: TestCoreFunctionStore): Slice[Memory] = {
    var keyUsed = keyValues.head.key.readInt() - 1
    val updateSlice = Slice.allocate[Memory](keyValues.size)

    keyValues foreach {
      keyValue =>
        if (randomlyDropUpdates && randomBoolean()) {
          keyUsed = keyValue.key.readInt()
        } else if (keyUsed < keyValue.key.readInt()) {
          eitherOne(
            left = {
              keyUsed = keyValue.key.readInt()
              Some(randomUpdateKeyValue(keyValue.key, updatedValue, deadline = deadline))
            },
            mid = {
              keyUsed = keyValue.key.readInt() + 10
              Some(
                randomRangeKeyValue(
                  from = keyValue.key,
                  to = keyUsed + 1,
                  fromValue =
                    randomFromValueOption(
                      value = updatedValue,
                      deadline = deadline,
                      addRemoves = false,
                      functionOutput = SegmentFunctionOutput.Update(updatedValue, deadline),
                      addPut = false
                    ),
                  rangeValue =
                    randomRangeValue(
                      value = updatedValue,
                      deadline = deadline,
                      functionOutput = SegmentFunctionOutput.Update(updatedValue, deadline),
                      addRemoves = false
                    )
                )
              )
            },
            right = {
              keyUsed = keyValue.key.readInt()
              Some(
                randomFunctionKeyValue(
                  key = keyValue.key,
                  output = SegmentFunctionOutput.Update(updatedValue, deadline)
                )
              )
            }
          ) foreach updateSlice.add
        }
    }

    updateSlice
  }

  implicit class IterableMemoryImplicits(keyValues: Iterable[Memory]) {

    /**
     * Returns the next key from the [[keyValues]] last key.
     *
     * Eg:
     * if lastKey == 10 and lastKey value is [[Memory.Fixed]] nextKey is 11
     * if lastKey == 10 and lastKey value is [[Memory.Range]] nextKey is 10
     */
    def nextKey(incrementBy: Int = 0): Int =
      keyValues.last match {
        case fixed: Memory.Fixed =>
          fixed.key.readInt() + 1 + incrementBy

        case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
          toKey.readInt() + incrementBy
      }
  }

  def mergeKeyValues(newKeyValues: Slice[KeyValue],
                     oldKeyValues: Slice[KeyValue],
                     isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                           timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: TestCoreFunctionStore): Iterable[Memory] = {
    val builder = MergeStats.random()

    import functionStore._

    KeyValueMerger.merge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = isLastLevel,
      initialiseIteratorsInOneSeek = randomBoolean()
    )

    builder.keyValues
  }


  def collapseMerge(newKeyValue: Memory.Fixed,
                    oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: CoreFunctionStore): KeyValue.Fixed =
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
            FixedMerger(newer, older.toMemory(newKeyValue.key)).runRandomIO.get match {
              case newPendingApply: KeyValue.PendingApply =>
                val resultApplies = newPendingApply.getOrFetchApplies.runRandomIO.get.reverse.toList ++ reveredApplied.drop(count)
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


}
