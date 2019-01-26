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

import java.nio.file.{Path, Paths}
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Random, Try}
import swaydb.compression.CompressionInternal
import swaydb.core.CommonAssertions._
import swaydb.core.TestLimitQueues.fileOpenLimiter
import swaydb.core.TryAssert._
import swaydb.core.data.KeyValue.{ReadOnly, WriteOnly}
import swaydb.core.data.Transient.Range
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.finder._
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.DBFile
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment
import swaydb.core.util.{TryUtil, UUIDUtil}
import swaydb.data.accelerate.Accelerator
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

object TestData {

  /**
    * Sequential time bytes generator.
    */

  val falsePositiveRate = 0.01

  implicit val functionStore: FunctionStore = FunctionStore.memory()

  implicit def toMemory(slice: Slice[KeyValue.WriteOnly])(implicit keyOrder: KeyOrder[Slice[Byte]]) = slice.toMemory

  def randomNextInt(max: Int) =
    Math.abs(Random.nextInt(max))

  implicit class ReopenSegment(segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                 ec: ExecutionContext,
                                                 keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                 fileOpenLimiter: DBFile => Unit = fileOpenLimiter,
                                                 timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                 groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(randomNextInt(1000))) {

    def tryReopen: Try[Segment] =
      tryReopen(segment.path)

    def tryReopen(path: Path, removeDeletes: Boolean = segment.removeDeletes): Try[Segment] =
      Segment(
        path = path,
        mmapReads = Random.nextBoolean(),
        mmapWrites = Random.nextBoolean(),
        minKey = segment.minKey,
        maxKey = segment.maxKey,
        segmentSize = segment.segmentSize,
        removeDeletes = removeDeletes,
        nearestExpiryDeadline = segment.nearestExpiryDeadline
      ) flatMap {
        reopenedSegment =>
          segment.close map {
            _ =>
              reopenedSegment
          }
      }

    def reopen: Segment =
      tryReopen.assertGet

    def reopen(path: Path, removeDeletes: Boolean = segment.removeDeletes): Segment =
      tryReopen(path, removeDeletes).assertGet
  }

  implicit class ReopenLevel(level: Level)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                           ec: ExecutionContext,
                                           timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                           keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                           compression: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(randomNextInt(1000))) {

    import swaydb.core.util.TryUtil._

    def putKeyValues(keyValues: Slice[KeyValue.ReadOnly]): Try[Unit] =
      if (keyValues.isEmpty)
        TryUtil.successUnit
      else
        Segment.copyToMemory(keyValues, Paths.get("testMemorySegment"), false, 1000.mb, TestData.falsePositiveRate, true) flatMap {
          segments =>
            segments tryMap {
              segment =>
                level.put(segment)
            } map {
              _ => ()
            }
        }

    def reopen: Level =
      reopen()

    def tryReopen: Try[Level] =
      tryReopen()

    def reopen(segmentSize: Long = level.segmentSize,
               throttle: LevelMeter => Throttle = level.throttle)(implicit keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                                  fileOpenLimiter: DBFile => Unit = fileOpenLimiter): Level =
      tryReopen(segmentSize, throttle).assertGet

    def tryReopen(segmentSize: Long = level.segmentSize,
                  throttle: LevelMeter => Throttle = level.throttle)(implicit keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                                     fileOpenLimiter: DBFile => Unit = fileOpenLimiter): Try[Level] =
      level.releaseLocks flatMap {
        _ =>
          level.closeSegments flatMap {
            _ =>
              Level(
                levelStorage = LevelStorage.Persistent(
                  mmapSegmentsOnWrite = level.mmapSegmentsOnWrite,
                  mmapSegmentsOnRead = level.mmapSegmentsOnRead,
                  dir = level.paths.headPath,
                  otherDirs = level.dirs.drop(1).map(dir => Dir(dir.path, 1))
                ),
                appendixStorage = AppendixStorage.Persistent(mmap = true, 4.mb),
                segmentSize = segmentSize,
                nextLevel = level.nextLevel,
                pushForward = level.pushForward,
                bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
                throttle = throttle,
                compressDuplicateValues = level.compressDuplicateValues
              ).map(_.asInstanceOf[Level])
          }
      }
  }

  implicit class ReopenLevelZero(level: LevelZero)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                   ec: ExecutionContext) {

    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

    def reopen: LevelZero =
      reopen()

    def reopen(mapSize: Long = level.maps.map.size)(implicit keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter,
                                                    timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                    fileOpenLimiter: DBFile => Unit = fileOpenLimiter): LevelZero = {
      val reopened =
        level.releaseLocks flatMap {
          _ =>
            level.closeSegments flatMap {
              _ =>
                LevelZero(
                  mapSize = mapSize,
                  storage = Level0Storage.Persistent(true, level.path.getParent, RecoveryMode.ReportFailure),
                  nextLevel = level.nextLevel,
                  acceleration = Accelerator.brake(),
                  readRetryLimit = 10000,
                  throttleOn = level.throttleOn
                )
            }
        }
      reopened.assertGet
    }

    def putKeyValues(keyValues: Slice[KeyValue.ReadOnly]): Try[Unit] =
      if (keyValues.isEmpty)
        TryUtil.successUnit
      else
        keyValues.toMapEntry match {
          case Some(value) =>
            level.put(value) map (_ => ())
          case None =>
            TryUtil.successUnit
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
      applies map {
        case Value.Remove(deadline, time) =>
          Memory.Remove(key, deadline, time)
        case Value.Update(value, deadline, time) =>
          Memory.Update(key, value, deadline, time)
        case function: Value.Function =>
          Memory.PendingApply(key, Slice(function))
      }
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

    def toMemoryResponse: Slice[Memory.SegmentResponse] = {
      val slice = Slice.create[Memory.SegmentResponse](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toMemoryResponse
      }
      slice
    }
  }

  implicit class ToSlice[T: ClassTag](items: Iterable[T]) {
    def toSlice: Slice[T] =
      Slice.empty ++ items
  }

  implicit class WriteOnlyToMemory(keyValue: KeyValue.WriteOnly) {
    def toMemoryResponse: Memory.SegmentResponse =
      keyValue match {
        case fixed: KeyValue.WriteOnly.Fixed =>
          fixed match {
            case Transient.Remove(key, deadline, time, previous, falsePositiveRate) =>
              Memory.Remove(key, deadline, time)

            case Transient.Update(key, value, deadline, time, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.Update(key, value, deadline, time)

            case Transient.Put(key, value, deadline, time, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.Put(key, value, deadline, time)

            case Transient.Function(key, function, value, deadline, time, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.Function(key, function, time)

            case Transient.PendingApply(key, applies, value, previous, falsePositiveRate, compressDuplicateValues) =>
              Memory.PendingApply(key, applies)
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
    def toMemory: Slice[Memory] = {
      keyValues map {
        case readOnly: ReadOnly =>
          readOnly.toMemory
        case writeOnly: WriteOnly =>
          writeOnly.toMemory
      } toSlice
    }
  }

  implicit class ReadOnlyToMemory(keyValues: Iterable[KeyValue.ReadOnly]) {
    def toTransient(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                    keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] = {
      val slice = Slice.create[Transient](keyValues.size)

      keyValues foreach {
        keyValue =>
          slice add keyValue.toTransient.updateStats(TestData.falsePositiveRate, slice.lastOption).asInstanceOf[Transient]
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

  implicit class ReadOnlyKeyValueToMemory(keyValue: KeyValue.ReadOnly)(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                       keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter) {

    def toTransient: Transient =
      toTransient(None)

    def toTransient(previous: Option[Transient]): Transient = {
      keyValue match {
        case memory: Memory =>
          memory match {
            case fixed: Memory.Fixed =>
              fixed match {
                case Memory.Put(key, value, deadline, time) =>
                  Transient.Put(
                    key = key,
                    value = value,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = deadline,
                    compressDuplicateValues = true,
                    time = time
                  )

                case Memory.Update(key, value, deadline, time) =>
                  Transient.Update(
                    key = key,
                    value = value,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = deadline,
                    compressDuplicateValues = true,
                    time = time
                  )

                case Memory.Remove(key, deadline, time) =>
                  Transient.Remove(
                    key = key,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = deadline,
                    time = time
                  )

                case Memory.Function(key, function, time) =>
                  Transient.Function(
                    key = key,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = None,
                    time = time,
                    function = function,
                    value = Some(function),
                    compressDuplicateValues = true
                  )

                case Memory.PendingApply(key, applies) =>
                  Transient.PendingApply(
                    key = key,
                    applies = applies,
                    previous = previous,
                    falsePositiveRate = TestData.falsePositiveRate,
                    compressDuplicateValues = true
                  )
              }
            case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
              Transient.Range[Value.FromValue, Value.RangeValue](
                fromKey = fromKey,
                toKey = toKey,
                fromValue = fromValue,
                rangeValue = rangeValue,
                falsePositiveRate = TestData.falsePositiveRate,
                previous = previous
              )

            case group @ Memory.Group(fromKey, toKey, nearestDeadline, groupDecompressor, _) =>
              val keyValues: Iterable[KeyValue.WriteOnly] = group.segmentCache.getAll().assertGet.toTransient
              Transient.Group(
                keyValues = keyValues,
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                falsePositiveRate = 0,
                previous = previous
              ).assertGet
          }

        case persistent: Persistent =>
          persistent match {
            case persistent: Persistent.Fixed =>
              persistent match {
                case put @ Persistent.Put(key, deadline, valueReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.Put(
                    key = key,
                    value = put.getOrFetchValue.assertGetOpt,
                    deadline = deadline,
                    previous = previous,
                    falsePositiveRate = TestData.falsePositiveRate,
                    compressDuplicateValues = true,
                    time = time
                  )

                case put @ Persistent.Update(key, deadline, valueReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.Update(
                    key = key,
                    value = put.getOrFetchValue.assertGetOpt,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = deadline,
                    compressDuplicateValues = true,
                    time = time
                  )

                case function @ Persistent.Function(key, lazyFunctionReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.Function(
                    key = key,
                    value = function.getOrFetchValue,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = None,
                    compressDuplicateValues = true,
                    time = time,
                    function = lazyFunctionReader.getOrFetchFunction.assertGet
                  )

                case pendingApply @ Persistent.PendingApply(key, time, deadline, lazyPendingApplyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Transient.PendingApply(
                    key = key,
                    applies = pendingApply.getOrFetchApplies.assertGet,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    compressDuplicateValues = true
                  )

                case Persistent.Remove(_key, deadline, time, indexOffset, nextIndexOffset, nextIndexSize) =>
                  Transient.Remove(
                    key = _key,
                    falsePositiveRate = TestData.falsePositiveRate,
                    previous = previous,
                    deadline = deadline,
                    time = time
                  )
              }

            case range @ Persistent.Range(_fromKey, _toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValue.assertGet
              Transient.Range(
                fromKey = _fromKey,
                toKey = _toKey,
                fromValue = fromValue,
                rangeValue = rangeValue,
                falsePositiveRate = TestData.falsePositiveRate,
                previous = previous
              )

            case group: Persistent.Group =>
              val allKeyValues = group.segmentCache.getAll().assertGet.toTransient
              Transient.Group(
                keyValues = allKeyValues,
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                previous = previous,
                falsePositiveRate = TestData.falsePositiveRate
              ).assertGet
          }
      }
    }

    def toMemoryGroup =
      keyValue match {
        case Persistent.Group(minKey, maxKey, groupDecompressor, _, _, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength, deadline) =>
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

        case persistent: Persistent.SegmentResponse =>
          persistent.toMemoryResponse

        case persistent: Persistent.Group =>
          persistent.toMemoryGroup
      }
    }

    def toMemoryResponse: Memory.SegmentResponse = {
      keyValue match {
        case memory: Memory.SegmentResponse =>
          memory

        case persistent: Persistent =>
          persistent match {
            case persistent: Persistent.Fixed =>
              persistent match {
                case put @ Persistent.Put(key, deadline, valueReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.Put(key, put.getOrFetchValue.assertGetOpt, deadline, time)

                case put @ Persistent.Update(key, deadline, valueReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.Update(key, put.getOrFetchValue.assertGetOpt, deadline, time)

                case function @ Persistent.Function(key, lazyFunctionReader, time, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.Function(key, function.getOrFetchFunction.assertGet, time)

                case pendingApply @ Persistent.PendingApply(key, time, deadline, lazyPendingApplyValueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
                  Memory.PendingApply(key, pendingApply.getOrFetchApplies.assertGet)

                case Persistent.Remove(_key, deadline, time, indexOffset, nextIndexOffset, nextIndexSize) =>
                  Memory.Remove(_key, deadline, time)
              }

            case range @ Persistent.Range(_fromKey, _toKey, _, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength) =>
              val (fromValue, rangeValue) = range.fetchFromAndRangeValue.assertGet
              Memory.Range(_fromKey, _toKey, fromValue, rangeValue)
          }
      }
    }
  }

  def randomStringOption: Option[Slice[Byte]] =
    if (Random.nextBoolean())
      Some(randomString)
    else
      None

  def randomString =
    randomCharacters()

  def randomDeadlineOption: Option[Deadline] =
    randomDeadlineOption()

  def randomDeadlineOption(expired: Boolean = randomBoolean): Option[Deadline] =
    if (Random.nextBoolean())
      Some(randomDeadline(expired))
    else
      None

  def randomDeadline(expired: Boolean = randomBoolean): Deadline =
    if (expired && Random.nextBoolean())
      0.seconds.fromNow - (randomIntMax(30) + 10).seconds
    else
      (randomIntMax(60) max 30).seconds.fromNow

  def randomBoolean =
    Random.nextBoolean()

  def randomDeadUpdateOrExpiredPut(key: Slice[Byte]): Memory.Fixed =
    eitherOne(
      randomFixedKeyValue(key, includePuts = false),
      randomPutKeyValue(key, deadline = Some(expiredDeadline())),
    )

  def randomPutKeyValue(key: Slice[Byte],
                        value: Option[Slice[Byte]] = randomStringOption,
                        deadline: Option[Deadline] = randomDeadlineOption)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Put = {
    val put = Memory.Put(key, value, deadline, timeGenerator.nextTime)
    //println(put)
    put
  }

  def randomExpiredPutKeyValue(key: Slice[Byte],
                               value: Option[Slice[Byte]] = randomStringOption)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Put =
    randomPutKeyValue(key, value, deadline = Some(expiredDeadline()))

  def randomUpdateKeyValue(key: Slice[Byte],
                           value: Option[Slice[Byte]] = randomStringOption,
                           deadline: Option[Deadline] = randomDeadlineOption)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Update =
    Memory.Update(key, value, deadline, timeGenerator.nextTime)

  def randomRemoveKeyValue(key: Slice[Byte],
                           deadline: Option[Deadline] = randomDeadlineOption)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Remove =
    Memory.Remove(key, deadline, timeGenerator.nextTime)

  def randomRemoveAny(from: Slice[Byte],
                      to: Slice[Byte],
                      addFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.SegmentResponse =
    eitherOne(
      left = randomRemoveOrUpdateOrFunctionRemove(from, addFunctions),
      right = randomRemoveRange(from, to)
    )

  def randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): RangeValue = {
    val value = randomRemoveOrUpdateOrFunctionRemove(Slice.emptyBytes, addFunctions).toRangeValue().assertGet
    //println(value)
    value
  }

  def randomRemoveFunctionValue()(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.Function =
    randomFunctionKeyValue(Slice.emptyBytes, SwayFunctionOutput.Remove).toRangeValue().assertGet

  def randomFunctionValue(output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.Function =
    randomFunctionKeyValue(Slice.emptyBytes, SwayFunctionOutput.Remove).toRangeValue().assertGet

  def randomRemoveOrUpdateOrFunctionRemoveValueOption(addFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Option[RangeValue] =
    eitherOne(
      left = None,
      right = Some(randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions))
    )

  /**
    * Removes can occur by [[Memory.Remove]], [[Memory.Update]] with expiry or [[Memory.Function]] with remove output.
    */
  def randomRemoveOrUpdateOrFunctionRemove(key: Slice[Byte],
                                           addFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Fixed =
    if (randomBoolean)
      randomRemoveKeyValue(key, randomExpiredDeadlineOption())
    else if (randomBoolean && addFunctions)
      randomFunctionKeyValue(key, randomRemoveFunctionOutput())
    else
      randomUpdateKeyValue(key, randomStringOption, Some(expiredDeadline()))

  def randomRemoveFunctionOutput() =
    eitherOne(
      SwayFunctionOutput.Remove,
      SwayFunctionOutput.Expire(expiredDeadline()),
      SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline()))
    )

  def randomUpdateFunctionOutput() =
    eitherOne(
      SwayFunctionOutput.Expire(randomDeadline(false)),
      SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false))
    )

  def randomRemoveRange(from: Slice[Byte],
                        to: Slice[Byte],
                        addFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Range =
    randomRangeKeyValue(
      from = from,
      to = to,
      fromValue = randomRemoveOrUpdateOrFunctionRemoveValueOption(addFunctions),
      rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions)
    )

  /**
    * Creates remove ranges of random range slices slice for all input key-values.
    */
  def randomRemoveRanges(keyValues: Iterable[Memory])(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Iterator[Memory.Range] =
    keyValues
      .grouped(randomIntMax(100) max 1)
      .flatMap {
        groupKeyValues =>
          if (groupKeyValues.isEmpty)
            None
          else {
            val maxKeyInt = getMaxKey(groupKeyValues.last.toTransient).maxKey.readInt()
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
                                 value: Option[Slice[Byte]] = randomStringOption,
                                 deadline: Option[Deadline] = randomDeadlineOption,
                                 functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                                 includeFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()) =
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
                     swayFunction: SwayFunction)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function = {
    val functionId = UUIDUtil.randomIdNoHyphenBytes()
    functionStore.put(functionId, swayFunction)
    Memory.Function(key, functionId, timeGenerator.nextTime)
  }

  def randomFunctionKeyValue(key: Slice[Byte],
                             output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = randomSwayFunction(output)
    )

  def randomFunctionNoDeadlineKeyValue(key: Slice[Byte],
                                       output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = randomSwayFunctionNoDeadline(output)
    )

  def randomKeyFunctionKeyValue(key: Slice[Byte],
                                output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.Key(_ => output)
    )

  def randomKeyDeadlineFunctionKeyValue(key: Slice[Byte],
                                        output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.KeyDeadline((_, _) => output)
    )

  def randomKeyValueFunctionKeyValue(key: Slice[Byte],
                                     output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.KeyValue((_, _) => output)
    )

  def randomKeyValueDeadlineFunctionKeyValue(key: Slice[Byte],
                                             output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.KeyValueDeadline((_, _, _) => output)
    )

  def randomValueFunctionKeyValue(key: Slice[Byte],
                                  output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.Value(_ => output)
    )

  def randomValueDeadlineFunctionKeyValue(key: Slice[Byte],
                                          output: SwayFunctionOutput = randomFunctionOutput())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Function =
    createFunction(
      key = key,
      swayFunction = SwayFunction.ValueDeadline((_, _) => output)
    )

  def randomFunctionOutput(addRemoves: Boolean = randomBoolean, expiredDeadline: Boolean = randomBoolean): SwayFunctionOutput =
    if (addRemoves && Random.nextBoolean())
      SwayFunctionOutput.Remove
    else
      randomFunctionUpdateOutput(expiredDeadline)

  def randomFunctionUpdateOutput(expiredDeadline: Boolean = randomBoolean): SwayFunctionOutput =
    if (Random.nextBoolean())
      SwayFunctionOutput.Expire(randomDeadline(expiredDeadline))
    else
      SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(expiredDeadline))

  def randomRequiresKeyFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction.RequiresKey =
    Random.shuffle(
      Seq[SwayFunction.RequiresKey](
        SwayFunction.Key(_ => functionOutput),
        SwayFunction.KeyValue((_, _) => functionOutput),
        SwayFunction.KeyDeadline((_, _) => functionOutput),
        SwayFunction.KeyValueDeadline((_, _, _) => functionOutput)
      )
    ).head

  def randomRequiresKeyOnlyWithOptionDeadlineFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction.RequiresKey =
    Random.shuffle(
      Seq[SwayFunction.RequiresKey](
        SwayFunction.Key(_ => functionOutput),
        SwayFunction.KeyDeadline((_, _) => functionOutput)
      )
    ).head

  def randomValueOnlyFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction.RequiresValue =
    Random.shuffle(
      Seq[SwayFunction.RequiresValue](
        SwayFunction.Value(_ => functionOutput),
        SwayFunction.ValueDeadline((_, _) => functionOutput),
      )
    ).head

  def randomRequiresValueWithOptionalKeyAndDeadlineFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction.RequiresValue =
    Random.shuffle(
      Seq[SwayFunction.RequiresValue](
        SwayFunction.Value(_ => functionOutput),
        SwayFunction.KeyValueDeadline((_, _, _) => functionOutput),
        SwayFunction.KeyValue((_, _) => functionOutput),
        SwayFunction.ValueDeadline((_, _) => functionOutput)
      )
    ).head

  def randomSwayFunctionNoDeadline(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction =
    Random.shuffle(
      Seq(
        SwayFunction.Value(_ => functionOutput),
        SwayFunction.Key(_ => functionOutput),
        SwayFunction.KeyValue((_, _) => functionOutput)
      )
    ).head

  def randomRequiresDeadlineFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction.RequiresDeadline =
    Random.shuffle(
      Seq[SwayFunction.RequiresDeadline](
        SwayFunction.KeyDeadline((_, _) => functionOutput),
        SwayFunction.KeyValueDeadline((_, _, _) => functionOutput),
        SwayFunction.ValueDeadline((_, _) => functionOutput)
      )
    ).head

  implicit class FunctionOutputImplicits(functionOutput: SwayFunctionOutput) {
    def toMemory(key: Slice[Byte],
                 time: Time) = {
      val outputFixed =
        functionOutput match {
          case SwayFunctionOutput.Remove =>
            Memory.Remove(key, None, time)

          case SwayFunctionOutput.Expire(deadline) =>
            Memory.Remove(key, Some(deadline), time)

          case SwayFunctionOutput.Update(newValue, newDeadline) =>
            Memory.Update(key, newValue, newDeadline, time)
        }
      //println(s"outputFixed: $outputFixed")
      outputFixed
    }
  }

  def randomSwayFunction(functionOutput: SwayFunctionOutput = randomFunctionOutput()): SwayFunction =
    if (Random.nextBoolean())
      randomRequiresKeyFunction(functionOutput)
    else
      randomValueOnlyFunction(functionOutput)

  def randomFunctionId(functionOutput: SwayFunctionOutput = randomFunctionOutput()): Slice[Byte] = {
    val functionId: Slice[Byte] = UUIDUtil.randomIdNoHyphenBytes()
    functionStore.put(functionId, randomSwayFunction(functionOutput))
    functionId
  }

  def randomApply(value: Option[Slice[Byte]] = randomStringOption,
                  deadline: Option[Deadline] = randomDeadlineOption,
                  addRemoves: Boolean = randomBoolean,
                  functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                  includeFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()) =
    if (addRemoves && Random.nextBoolean())
      Value.Remove(deadline, timeGenerator.nextTime)
    else if (includeFunctions && Random.nextBoolean())
      Value.Function(randomFunctionId(functionOutput), timeGenerator.nextTime)
    else
      Value.Update(value, deadline, timeGenerator.nextTime)

  def randomApplyWithDeadline(value: Option[Slice[Byte]] = randomStringOption,
                              addRandomRangeRemoves: Boolean = randomBoolean,
                              deadline: Deadline = randomDeadline())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()) =
    if (addRandomRangeRemoves && Random.nextBoolean())
      Value.Remove(Some(deadline), timeGenerator.nextTime)
    else
      Value.Update(value, Some(deadline), timeGenerator.nextTime)

  def randomApplies(max: Int = 5,
                    value: Option[Slice[Byte]] = randomStringOption,
                    deadline: Option[Deadline] = randomDeadlineOption,
                    addRemoves: Boolean = randomBoolean,
                    functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                    includeFunctions: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Slice[Value.Apply] =
    Slice {
      (1 to (Random.nextInt(max) max 1)).map {
        _ =>
          randomApply(
            value = value,
            deadline = deadline,
            addRemoves = addRemoves,
            functionOutput = functionOutput,
            includeFunctions = includeFunctions)
      } toArray
    }

  def randomAppliesWithDeadline(max: Int = 5,
                                value: Option[Slice[Byte]] = randomStringOption,
                                addRandomRangeRemoves: Boolean = randomBoolean,
                                deadline: Deadline = randomDeadline())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Slice[Value.Apply] =
    Slice {
      (1 to (Random.nextInt(max) max 1)).map {
        _ =>
          randomApplyWithDeadline(
            value = value,
            addRandomRangeRemoves = addRandomRangeRemoves,
            deadline = deadline
          )
      } toArray
    }

  def randomFixedKeyValue(key: Slice[Byte],
                          value: Option[Slice[Byte]] = randomStringOption,
                          deadline: Option[Deadline] = randomDeadlineOption,
                          functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                          includePendingApply: Boolean = true,
                          includeFunctions: Boolean = true,
                          includeRemoves: Boolean = true,
                          includePuts: Boolean = true,
                          moveAppliesTimeBackward: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Fixed =
    if (includePuts && Random.nextBoolean())
      Memory.Put(key, value, deadline, timeGenerator.nextTime)
    else if (includeRemoves && Random.nextBoolean())
      Memory.Remove(key, deadline, timeGenerator.nextTime)
    else if (includeFunctions && Random.nextBoolean())
      Memory.Function(key, randomFunctionId(functionOutput), timeGenerator.nextTime)
    else if (includePendingApply && Random.nextBoolean())
      Memory.PendingApply(
        key,
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
      Memory.Update(key, value, deadline, timeGenerator.nextTime)

  def randomCompression(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.random(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4OrSnappy(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.randomLZ4OrSnappy(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4(minCompressionPercentage: Double = Double.MinValue): CompressionInternal =
    CompressionInternal.randomLZ4(minCompressionPercentage = minCompressionPercentage)

  def randomRangeKeyValue(from: Slice[Byte],
                          to: Slice[Byte],
                          fromValue: Option[FromValue] = randomFromValueOption()(TestTimeGenerator.random),
                          rangeValue: RangeValue = randomRangeValue()(TestTimeGenerator.random)): Memory.Range = {
    val range = Memory.Range(from, to, fromValue, rangeValue)
    //println(range)
    range
  }

  def randomRangeKeyValueWithDeadline(from: Slice[Byte],
                                      to: Slice[Byte],
                                      fromValue: Option[FromValue] = randomFromValueWithDeadlineOption()(TestTimeGenerator.random),
                                      rangeValue: RangeValue = randomRangeValueWithDeadline()(TestTimeGenerator.random)): Memory.Range = {
    val range = Memory.Range(from, to, fromValue, rangeValue)
    //println(range)
    range
  }

  def randomRangeKeyValueWithFromValueExpiredDeadline(from: Slice[Byte],
                                                      to: Slice[Byte],
                                                      fromValue: Option[FromValue] = randomFromValueWithDeadlineOption(deadline = expiredDeadline())(TestTimeGenerator.random),
                                                      rangeValue: RangeValue = randomRangeValueWithDeadline()(TestTimeGenerator.random)): Memory.Range =
    randomRangeKeyValueWithDeadline(from, to, fromValue, rangeValue)

  def randomRangeKeyValueForDeadline(from: Slice[Byte],
                                     to: Slice[Byte],
                                     deadline: Deadline = randomDeadline()): Memory.Range =
    Memory.Range(
      fromKey = from,
      toKey = to,
      fromValue = randomFromValueWithDeadlineOption(deadline = deadline)(TestTimeGenerator.random),
      rangeValue = randomRangeValueWithDeadline(deadline = deadline)(TestTimeGenerator.random)
    )

  def randomRangeValueOption(from: Slice[Byte], to: Slice[Byte]): Option[Memory.Range] =
    if (Random.nextBoolean())
      Some(randomRangeKeyValue(from, to))
    else
      None

  def randomFromValueOption(value: Option[Slice[Byte]] = randomStringOption,
                            deadline: Option[Deadline] = randomDeadlineOption,
                            functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                            addRemoves: Boolean = Random.nextBoolean(),
                            addPut: Boolean = Random.nextBoolean())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Option[Value.FromValue] =
    if (Random.nextBoolean())
      Some(
        randomFromValue(
          value = value,
          addRemoves = addRemoves,
          functionOutput = functionOutput,
          deadline = deadline,
          addPut = addPut
        )
      )
    else
      None

  def randomFromValueWithDeadlineOption(value: Option[Slice[Byte]] = randomStringOption,
                                        addRandomRangeRemoves: Boolean = Random.nextBoolean(),
                                        deadline: Deadline = randomDeadline())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Option[Value.FromValue] =
    if (Random.nextBoolean())
      Some(randomFromValueWithDeadline(value, addRandomRangeRemoves, deadline))
    else
      None

  def randomUpdateRangeValue(value: Option[Slice[Byte]] = randomStringOption,
                             functionOutput: SwayFunctionOutput = randomUpdateFunctionOutput()) = {
    val addRemoves = randomBoolean
    val deadline =
      if (addRemoves)
        Some(randomDeadline(false))
      else
        randomDeadlineOption(false)

    randomRangeValue(value = value, addRemoves = addRemoves, functionOutput = functionOutput, deadline = deadline)
  }

  def randomFromValue(value: Option[Slice[Byte]] = randomStringOption,
                      addRemoves: Boolean = Random.nextBoolean(),
                      deadline: Option[Deadline] = randomDeadlineOption,
                      functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                      addPut: Boolean = Random.nextBoolean())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.FromValue =
    if (addPut && Random.nextBoolean())
      Value.Put(value, deadline, timeGenerator.nextTime)
    else
      randomRangeValue(value = value, addRemoves = addRemoves, functionOutput = functionOutput, deadline = deadline)

  def randomRangeValue(value: Option[Slice[Byte]] = randomStringOption,
                       deadline: Option[Deadline] = randomDeadlineOption,
                       functionOutput: SwayFunctionOutput = randomFunctionOutput(),
                       addRemoves: Boolean = Random.nextBoolean())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.RangeValue =
    if (addRemoves && Random.nextBoolean())
      Value.Remove(deadline, timeGenerator.nextTime)
    else if (Random.nextBoolean())
      Value.Function(randomFunctionId(functionOutput), timeGenerator.nextTime)
    else if (Random.nextBoolean())
      Value.PendingApply(randomApplies(value = value, addRemoves = addRemoves, deadline = deadline, functionOutput = functionOutput))
    else
      Value.Update(value, deadline, timeGenerator.nextTime)

  def randomFromValueWithDeadline(value: Option[Slice[Byte]] = randomStringOption,
                                  addRandomRangeRemoves: Boolean = Random.nextBoolean(),
                                  deadline: Deadline = randomDeadline())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.FromValue =
    if (Random.nextBoolean())
      Value.Put(value, Some(deadline), timeGenerator.nextTime)
    else
      randomRangeValueWithDeadline(value = value, addRandomRangeRemoves = addRandomRangeRemoves, deadline = deadline)

  def randomRangeValueWithDeadline(value: Option[Slice[Byte]] = randomStringOption,
                                   addRandomRangeRemoves: Boolean = Random.nextBoolean(),
                                   deadline: Deadline = randomDeadline())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Value.RangeValue =
    if (addRandomRangeRemoves && Random.nextBoolean())
      Value.Remove(Some(deadline), timeGenerator.nextTime)
    else if (Random.nextBoolean())
      Value.PendingApply(randomAppliesWithDeadline(value = value, deadline = deadline))
    else
      Value.Update(value, Some(deadline), timeGenerator.nextTime)

  def randomCharacters(size: Int = 10) = Random.alphanumeric.take(size max 1).mkString

  def randomBytes(size: Int = 10) = Array.fill(size)(randomByte())

  def randomByteChunks(size: Int = 10, sizePerChunk: Int = 10): Seq[Slice[Byte]] =
    (1 to size) map {
      _ =>
        Slice(randomBytes(sizePerChunk))
    }

  def randomBytesSlice(size: Int = 10) = Slice(randomBytes(size))

  def randomByte() = (Random.nextInt(256) - 128).toByte

  def ints(numbers: Int): Int =
    (1 to numbers).foldLeft("") {
      case (concat, _) =>
        concat + Math.abs(Random.nextInt(9)).toString
    }.toInt

  def randomInt(minus: Int = 0) = Math.abs(Random.nextInt(Int.MaxValue)) - minus - 1

  def randomIntMax(max: Int = Int.MaxValue) =
    Math.abs(Random.nextInt(max))

  def randomIntMin(min: Int) =
    Math.abs(randomIntMax()) max min

  def randomIntMaxOption(max: Int = Int.MaxValue) =
    if (Random.nextBoolean())
      Some(randomIntMax(max))
    else
      None

  def randomIntKeyStringValues(count: Int = 5,
                               startId: Option[Int] = None,
                               valueSize: Int = 50,
                               addRandomRemoves: Boolean = false,
                               addRandomRanges: Boolean = false,
                               addRandomRemoveDeadlines: Boolean = false,
                               addRandomPutDeadlines: Boolean = false)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental(),
                                                                       keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                       keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addRandomRemoves = addRandomRemoves,
      addRandomRanges = addRandomRanges,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines
    )

  def randomizedKeyValues(count: Int = 5,
                          startId: Option[Int] = None,
                          valueSize: Int = 50,
                          addPut: Boolean = true,
                          addRandomRemoves: Boolean = Random.nextBoolean(),
                          addRandomRangeRemoves: Boolean = Random.nextBoolean(),
                          addRandomUpdates: Boolean = Random.nextBoolean(),
                          addRandomFunctions: Boolean = Random.nextBoolean(),
                          addRandomRanges: Boolean = Random.nextBoolean(),
                          addRandomPendingApply: Boolean = Random.nextBoolean(),
                          addRandomRemoveDeadlines: Boolean = Random.nextBoolean(),
                          addRandomPutDeadlines: Boolean = Random.nextBoolean(),
                          addRandomExpiredPutDeadlines: Boolean = Random.nextBoolean(),
                          addRandomUpdateDeadlines: Boolean = Random.nextBoolean(),
                          addRandomGroups: Boolean = Random.nextBoolean())(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental(),
                                                                           keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                           keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addPut = addPut,
      addRandomRemoves = addRandomRemoves,
      addRandomRangeRemoves = addRandomRangeRemoves,
      addRandomUpdates = addRandomUpdates,
      addRandomFunctions = addRandomFunctions,
      addRandomRanges = addRandomRanges,
      addRandomPendingApply = addRandomPendingApply,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines,
      addRandomExpiredPutDeadlines = addRandomExpiredPutDeadlines,
      addRandomUpdateDeadlines = addRandomUpdateDeadlines,
      addRandomGroups = addRandomGroups
    )

  def groupsOnly(count: Int = 5,
                 startId: Option[Int] = None,
                 valueSize: Int = 50,
                 nonValue: Boolean = false)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental(),
                                            keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                            keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addRandomGroups = true
    )

  def randomPutKeyValues(count: Int = 5,
                         startId: Option[Int] = None,
                         valueSize: Int = 50,
                         addRandomRemoves: Boolean = false,
                         addRandomRanges: Boolean = false,
                         addRandomRemoveDeadlines: Boolean = false,
                         addRandomPutDeadlines: Boolean = true,
                         addRandomExpiredPutDeadlines: Boolean = false)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.random): Slice[Memory] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = valueSize,
      addPut = true,
      addRandomRemoves = addRandomRemoves,
      addRandomRanges = addRandomRanges,
      addRandomExpiredPutDeadlines = addRandomExpiredPutDeadlines,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines
    ).toMemory

  def randomKeyValues(count: Int = 20,
                      startId: Option[Int] = None,
                      valueSize: Int = 50,
                      addPut: Boolean = true,
                      addRandomRemoves: Boolean = false,
                      addRandomRangeRemoves: Boolean = false,
                      addRandomUpdates: Boolean = false,
                      addRandomFunctions: Boolean = false,
                      addRandomRemoveDeadlines: Boolean = false,
                      addRandomPendingApply: Boolean = false,
                      addRandomPutDeadlines: Boolean = false,
                      addRandomExpiredPutDeadlines: Boolean = false,
                      addRandomUpdateDeadlines: Boolean = false,
                      addRandomRanges: Boolean = false,
                      addRandomGroups: Boolean = false)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental(),
                                                        keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                        keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[Transient] = {
    val slice = Slice.create[Transient](count * 50) //extra space because addRandomRanges and random Groups can be added for Fixed and Range key-values in the same iteration.
    //            var key = 1
    var key = startId getOrElse randomInt(minus = count)
    val until = key + count
    while (key < until) {
      if (addRandomGroups && Random.nextBoolean()) {
        //create a Random group with the inner key-values the same as count of this group.
        val groupKeyValues =
          randomKeyValues(
            count = randomIntMax((count max 10) max 50),
            startId = Some(key),
            valueSize = valueSize,
            addPut = addPut,
            addRandomRemoves = addRandomRemoves,
            addRandomRangeRemoves = addRandomRangeRemoves,
            addRandomFunctions = addRandomFunctions,
            addRandomUpdates = addRandomUpdates,
            addRandomRemoveDeadlines = addRandomRemoveDeadlines,
            addRandomExpiredPutDeadlines = addRandomExpiredPutDeadlines,
            addRandomPendingApply = addRandomPendingApply,
            addRandomPutDeadlines = addRandomPutDeadlines,
            addRandomUpdateDeadlines = addRandomUpdateDeadlines,
            addRandomRanges = addRandomRanges,
            addRandomGroups = false //do not create more inner groups.
          )

        //could be possible that randomKeyValues returns empty if all generations were set to false.
        if (groupKeyValues.isEmpty) {
          if (Random.nextBoolean()) key += 1
        } else {
          Transient.Group(groupKeyValues, randomCompression(), randomCompression(), TestData.falsePositiveRate, previous = slice.lastOption).assertGetOpt match {
            case Some(group) =>
              slice add group
              //randomly skip the Group's toKey for the next key. Next key should not be the same as toKey so add a minimum of 1 to next key.
              if (Random.nextBoolean())
                key = group.maxKey.maxKey.readInt() + 1
              else
                key = group.maxKey.maxKey.readInt() + 1 + randomIntMax(5)
            case None =>
              //if it's empty randomly incrementing the key and continue.
              if (Random.nextBoolean())
                key += 1
          }
        }

      } else if (addRandomRanges && Random.nextBoolean()) {
        val toKey = key + 10
        val fromValueValueBytes = eitherOne(None, Some(randomBytesSlice(valueSize)))
        val rangeValueValueBytes = eitherOne(None, Some(randomBytesSlice(valueSize)))
        val fromValueDeadline =
          if (addRandomPutDeadlines || addRandomRemoveDeadlines || addRandomUpdateDeadlines)
            randomDeadlineOption(addRandomExpiredPutDeadlines)
          else
            None
        val rangeValueDeadline = if (addRandomRemoveDeadlines || addRandomUpdateDeadlines) randomDeadlineOption else None
        slice add randomRangeKeyValue(
          from = key,
          to = toKey,
          fromValue = randomFromValueOption(value = fromValueValueBytes, deadline = fromValueDeadline, addPut = addPut),
          rangeValue = randomRangeValue(value = rangeValueValueBytes, addRemoves = addRandomRangeRemoves, deadline = rangeValueDeadline)
        ).toTransient(slice.lastOption)
        //randomly skip the Range's toKey for the next key.
        if (Random.nextBoolean())
          key = toKey
        else
          key = toKey + randomIntMax(5)
      } else if (addRandomRemoves && Random.nextBoolean()) {
        slice add randomRemoveKeyValue(key, if (addRandomRemoveDeadlines) randomDeadlineOption else None).toTransient(slice.lastOption)
        key = key + 1
      } else if (addRandomUpdates && Random.nextBoolean()) {
        val valueBytes = if (valueSize == 0) None else eitherOne(None, Some(randomBytesSlice(valueSize)))
        slice add randomUpdateKeyValue(key = key: Slice[Byte], deadline = if (addRandomUpdateDeadlines) randomDeadlineOption else None, value = valueBytes).toTransient(slice.lastOption)
        key = key + 1
      } else if (addRandomFunctions && Random.nextBoolean()) {
        slice add randomFunctionKeyValue(key = key: Slice[Byte]).toTransient(slice.lastOption)
        key = key + 1
      } else if (addRandomPendingApply && Random.nextBoolean()) {
        val valueBytes = if (valueSize == 0) None else eitherOne(None, Some(randomBytesSlice(valueSize)))
        slice add randomPendingApplyKeyValue(key = key: Slice[Byte], deadline = if (addRandomUpdateDeadlines) randomDeadlineOption else None, value = valueBytes).toTransient(slice.lastOption)
        key = key + 1
      } else if (addPut) {
        val valueBytes = if (valueSize == 0) None else eitherOne(None, Some(randomBytesSlice(valueSize)))
        val deadline = if (addRandomPutDeadlines) randomDeadlineOption(addRandomExpiredPutDeadlines) else None
        slice add randomPutKeyValue(key = key: Slice[Byte], deadline = deadline, value = valueBytes).toTransient(slice.lastOption)
        key = key + 1
      } else {
        key = key + 1
      }
    }
    slice.close()
  }

  def randomFixedNoneValue(count: Int = 20,
                           startId: Option[Int] = None,
                           addRandomUpdates: Boolean = true,
                           addRandomUpdateDeadlines: Boolean = true,
                           addRandomPutDeadlines: Boolean = true,
                           addRandomRemoves: Boolean = true,
                           addRandomRemoveDeadlines: Boolean = true)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental(),
                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                     keyValueLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter): Slice[KeyValue.WriteOnly] =
    randomKeyValues(
      count = count,
      startId = startId,
      valueSize = 0,
      addRandomUpdates = addRandomUpdates,
      addRandomUpdateDeadlines = addRandomUpdateDeadlines,
      addRandomPutDeadlines = addRandomPutDeadlines,
      addRandomRemoves = addRandomRemoves,
      addRandomRemoveDeadlines = addRandomRemoveDeadlines)

  def randomGroup(keyValues: Slice[KeyValue.WriteOnly] = randomizedKeyValues()(TestTimeGenerator.random, KeyOrder.default, TestLimitQueues.keyValueLimiter),
                  keyCompression: CompressionInternal = randomCompression(),
                  valueCompression: CompressionInternal = randomCompression(),
                  falsePositiveRate: Double = TestData.falsePositiveRate,
                  previous: Option[KeyValue.WriteOnly] = None)(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Transient.Group =
    Transient.Group(
      keyValues = keyValues,
      indexCompression = keyCompression,
      valueCompression = valueCompression,
      falsePositiveRate = falsePositiveRate,
      previous = previous
    ).assertGet

  implicit class MemoryTypeImplicits(memory: Memory.type) {

    /**
      * Memory.Put
      */
    def put(key: Slice[Byte],
            value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, Some(value), None, timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAt: Deadline)(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, Some(value), Some(removeAt), timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]],
            removeAt: Deadline)(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, value, Some(removeAt), timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAt: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, Some(value), removeAt, timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, Some(value), Some(removeAfter.fromNow), timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]])(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, value, None, timeGenerator.nextTime)

    def put(key: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Memory.Put =
      Memory.Put(key, None, None, timeGenerator.nextTime)

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]],
            deadline: Option[Deadline],
            time: Time): Memory.Put =
      Memory.Put(key, value, deadline, time)

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]],
            deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Put =
      Memory.Put(key, value, deadline, timeGenerator.nextTime)

    /**
      * Memory.Update
      */
    def update(key: Slice[Byte],
               value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, Some(value), None, timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAt: Deadline)(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, Some(value), Some(removeAt), timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Option[Slice[Byte]],
               removeAt: Deadline)(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, value, Some(removeAt), timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAt: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, Some(value), removeAt, timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, Some(value), Some(removeAfter.fromNow), timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Option[Slice[Byte]])(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, value, None, timeGenerator.nextTime)

    def update(key: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Memory.Update =
      Memory.Update(key, None, None, timeGenerator.nextTime)

    def update(key: Slice[Byte],
               value: Option[Slice[Byte]],
               deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Update =
      Memory.Update(key, value, deadline, timeGenerator.nextTime)

    /**
      * Memory.Remove
      */

    def remove(key: Slice[Byte]): Memory.Remove =
      Memory.Remove(key, None, Time.empty)

    def remove(key: Slice[Byte], deadline: Deadline): Memory.Remove =
      Memory.Remove(key, Some(deadline), Time.empty)

    def remove(key: Slice[Byte], deadline: FiniteDuration): Memory.Remove =
      Memory.Remove(key, Some(deadline.fromNow), Time.empty)

    def remove(key: Slice[Byte],
               deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator = TestTimeGenerator.Incremental()): Memory.Remove =
      Memory.Remove(key, deadline, timeGenerator.nextTime)
  }

  implicit class TransientTypeImplicits(transient: Transient.type) {

    /**
      * Transient.Remove
      *
      * @param key
      * @return
      */
    def remove(key: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Transient.Remove =
      Transient.Remove(
        key = key,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        previous = None,
        deadline = None
      )

    def remove(key: Slice[Byte],
               removeAfter: FiniteDuration,
               falsePositiveRate: Double)(implicit timeGenerator: TestTimeGenerator): Transient.Remove =
      Transient.Remove(
        key = key,
        falsePositiveRate = falsePositiveRate,
        previous = None,
        deadline = Some(removeAfter.fromNow),
        time = timeGenerator.nextTime
      )

    def remove(key: Slice[Byte],
               falsePositiveRate: Double)(implicit timeGenerator: TestTimeGenerator): Transient.Remove =
      Transient.Remove(
        key = key,
        falsePositiveRate = falsePositiveRate,
        previous = None,
        deadline = None,
        time = timeGenerator.nextTime
      )

    def remove(key: Slice[Byte],
               falsePositiveRate: Double,
               previous: Option[KeyValue.WriteOnly])(implicit timeGenerator: TestTimeGenerator): Transient.Remove =
      Transient.Remove(
        key = key,
        falsePositiveRate = falsePositiveRate,
        previous = previous,
        deadline = None,
        time = timeGenerator.nextTime
      )

    def remove(key: Slice[Byte],
               falsePositiveRate: Double,
               previous: Option[KeyValue.WriteOnly],
               deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Transient.Remove =
      Transient.Remove(
        key = key,
        deadline = deadline,
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime
      )

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]],
            falsePositiveRate: Double,
            previousMayBe: Option[KeyValue.WriteOnly])(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = value,
        deadline = None,
        time = timeGenerator.nextTime,
        previous = previousMayBe,
        falsePositiveRate = falsePositiveRate,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Option[Slice[Byte]],
            falsePositiveRate: Double,
            previousMayBe: Option[KeyValue.WriteOnly],
            deadline: Option[Deadline],
            compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = value,
        deadline = deadline,
        previous = previousMayBe,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

    def put(key: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = None,
        deadline = None,
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = None,
        previous = None,
        time = timeGenerator.nextTime,
        falsePositiveRate = TestData.falsePositiveRate,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = Some(removeAfter.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Slice[Byte],
            deadline: Deadline)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = Some(deadline),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = None,
        deadline = Some(removeAfter.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Slice[Byte],
            removeAfter: Option[FiniteDuration])(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = removeAfter.map(_.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            value: Slice[Byte],
            falsePositiveRate: Double)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = None,
        previous = None,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def put(key: Slice[Byte],
            previous: Option[KeyValue.WriteOnly],
            deadline: Option[Deadline],
            compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = None,
        deadline = deadline,
        previous = previous,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

    def put(key: Slice[Byte],
            value: Slice[Byte],
            falsePositiveRate: Double,
            previous: Option[KeyValue.WriteOnly],
            compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Put =
      Transient.Put(
        key = key,
        value = Some(value),
        deadline = None,
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

    def update(key: Slice[Byte],
               value: Option[Slice[Byte]],
               falsePositiveRate: Double,
               previousMayBe: Option[KeyValue.WriteOnly])(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = value,
        deadline = None,
        time = timeGenerator.nextTime,
        previous = previousMayBe,
        falsePositiveRate = falsePositiveRate,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Option[Slice[Byte]],
               falsePositiveRate: Double,
               previousMayBe: Option[KeyValue.WriteOnly],
               deadline: Option[Deadline],
               compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = value,
        deadline = deadline,
        previous = previousMayBe,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

    def update(key: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = None,
        deadline = None,
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = None,
        previous = None,
        time = timeGenerator.nextTime,
        falsePositiveRate = TestData.falsePositiveRate,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = Some(removeAfter.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Slice[Byte],
               deadline: Deadline)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = Some(deadline),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               removeAfter: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = None,
        deadline = Some(removeAfter.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Slice[Byte],
               removeAfter: Option[FiniteDuration])(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = removeAfter.map(_.fromNow),
        previous = None,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               value: Slice[Byte],
               falsePositiveRate: Double)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = None,
        previous = None,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = true
      )

    def update(key: Slice[Byte],
               previous: Option[KeyValue.WriteOnly],
               deadline: Option[Deadline],
               compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = None,
        deadline = deadline,
        previous = previous,
        falsePositiveRate = TestData.falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

    def update(key: Slice[Byte],
               value: Slice[Byte],
               falsePositiveRate: Double,
               previous: Option[KeyValue.WriteOnly],
               compressDuplicateValues: Boolean)(implicit timeGenerator: TestTimeGenerator): Transient.Update =
      Transient.Update(
        key = key,
        value = Some(value),
        deadline = None,
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        time = timeGenerator.nextTime,
        compressDuplicateValues = compressDuplicateValues
      )

  }

  implicit class ValueUpdateTypeImplicits(remove: Value.type) {

    def remove(deadline: Option[Deadline],
               time: Time): Value.Remove =
      Value.Remove(deadline, time)

    def remove(deadline: Deadline)(implicit timeGenerator: TestTimeGenerator): Value.Remove =
      Value.Remove(Some(deadline), timeGenerator.nextTime)

    def remove(deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Value.Remove =
      Value.Remove(deadline, timeGenerator.nextTime)

    def put(value: Option[Slice[Byte]],
            deadline: Option[Deadline],
            time: Time)(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(Some(value), deadline, time)

    def put(value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(Some(value), None, timeGenerator.nextTime)

    def put(value: Option[Slice[Byte]])(removeAfter: Deadline)(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(value, Some(removeAfter), timeGenerator.nextTime)

    def put(value: Slice[Byte], removeAfter: Deadline)(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(Some(value), Some(removeAfter), timeGenerator.nextTime)

    def put(value: Option[Slice[Byte]], removeAfter: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(value, removeAfter, timeGenerator.nextTime)

    def put(value: Slice[Byte], duration: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(Some(value), Some(duration.fromNow), timeGenerator.nextTime)

    def put(value: Option[Slice[Byte]], duration: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Value.Put =
      Value.Put(value, Some(duration.fromNow), timeGenerator.nextTime)

    def update(value: Option[Slice[Byte]],
               deadline: Option[Deadline],
               time: Time): Value.Update =
      Value.Update(Some(value), deadline, time)

    def update(value: Slice[Byte])(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(Some(value), None, timeGenerator.nextTime)

    def update(value: Slice[Byte], deadline: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(Some(value), deadline, timeGenerator.nextTime)

    def update(value: Option[Slice[Byte]])(removeAfter: Deadline)(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(value, Some(removeAfter), timeGenerator.nextTime)

    def update(value: Slice[Byte], removeAfter: Deadline)(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(Some(value), Some(removeAfter), timeGenerator.nextTime)

    def update(value: Option[Slice[Byte]], removeAfter: Option[Deadline])(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(value, removeAfter, timeGenerator.nextTime)

    def update(value: Slice[Byte], duration: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(Some(value), Some(duration.fromNow), timeGenerator.nextTime)

    def update(value: Option[Slice[Byte]], duration: FiniteDuration)(implicit timeGenerator: TestTimeGenerator): Value.Update =
      Value.Update(value, Some(duration.fromNow), timeGenerator.nextTime)
  }

  implicit class RangeTypeImplicits(range: Transient.Range.type) {
    def create[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                            toKey: Slice[Byte],
                                                            fromValue: Option[F],
                                                            rangeValue: R)(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range =
      Range(
        fromKey = fromKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        falsePositiveRate = TestData.falsePositiveRate,
        previous = None
      )
  }

  def collectUsedDeadlines(keyValues: Slice[KeyValue.WriteOnly], usedDeadlines: List[Deadline]): List[Deadline] =
    keyValues.foldLeft(usedDeadlines) {
      case (usedDeadlines, keyValue) =>
        keyValue match {
          case remove: Transient.Remove =>
            usedDeadlines ++ remove.deadline
          case put: Transient.Put =>
            usedDeadlines ++ put.deadline
          case update: Transient.Update =>
            usedDeadlines ++ update.deadline
          case _: Transient.Function =>
            usedDeadlines
          case apply: Transient.PendingApply =>
            collectUsedDeadlines(apply.applies.map(_.toMemory(Slice.emptyBytes)).map(_.toTransient), usedDeadlines)
          case range: Transient.Range =>
            val fromTransient = range.fromValue.map(_.toMemory(Slice.emptyBytes).toTransient)
            val rangeTransient = range.rangeValue.toMemory(Slice.emptyBytes).toTransient
            collectUsedDeadlines(Slice(rangeTransient) ++ fromTransient, usedDeadlines)
          case group: Transient.Group =>
            collectUsedDeadlines(group.keyValues.map(_.asInstanceOf[Transient]).toSlice, usedDeadlines)
        }
    }

  def nearestDeadline(keyValues: Slice[KeyValue.WriteOnly]): Option[Deadline] = {
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

  def maxKey(keyValues: Slice[KeyValue.WriteOnly]): MaxKey[Slice[Byte]] =
    getMaxKey(keyValues.last)

  @tailrec
  def getMaxKey(transient: KeyValue.WriteOnly): MaxKey[Slice[Byte]] =
    transient match {
      case last: Transient.Remove =>
        MaxKey.Fixed(last.key)
      case last: Transient.Put =>
        MaxKey.Fixed(last.key)
      case last: Transient.Update =>
        MaxKey.Fixed(last.key)
      case last: Transient.Function =>
        MaxKey.Fixed(last.key)
      case last: Transient.PendingApply =>
        MaxKey.Fixed(last.key)
      case last: Transient.Range =>
        MaxKey.Range(last.fromKey, last.toKey)
      case last: Transient.Group =>
        getMaxKey(last.keyValues.last)
    }

  def unexpiredPuts(keyValues: Slice[KeyValue]): List[KeyValue.ReadOnly.Put] =
    unzipGroups(keyValues).flatMap {
      keyValue =>
        keyValue.asPut flatMap {
          put =>
            if (put.hasTimeLeft())
              Some(put)
            else
              None

        }
    }(collection.breakOut)

  /**
    * Randomly updates all key-values using one of the many update methods.
    *
    * Used for testing all updates work for all existing put key-values.
    */
  def randomUpdate(keyValues: Slice[Memory.Put],
                   updatedValue: Option[Slice[Byte]],
                   deadline: Option[Deadline],
                   randomlyDropUpdates: Boolean): Iterable[Memory] = {
    var keyUsed = keyValues.head.key.readInt() - 1
    keyValues map {
      keyValue =>
        if (randomlyDropUpdates && Random.nextBoolean()) {
          keyUsed = keyValue.key.readInt()
          None
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
                  fromValue = randomFromValueOption(
                    value = updatedValue,
                    deadline = deadline,
                    addRemoves = false,
                    functionOutput = SwayFunctionOutput.Update(updatedValue, deadline),
                    addPut = false
                  ),
                  rangeValue = randomRangeValue(
                    value = updatedValue,
                    deadline = deadline,
                    functionOutput = SwayFunctionOutput.Update(updatedValue, deadline),
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
                  output = SwayFunctionOutput.Update(updatedValue, deadline)
                )
              )
            }
          )
        } else {
          None
        }
    } flatten
  }

  implicit class HigherImplicits(higher: Higher.type) {
    def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentReader: CurrentFinder,
                                nextReader: NextFinder,
                                functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] =
      Higher(key, Seek.Next, Seek.Next)
  }
}

