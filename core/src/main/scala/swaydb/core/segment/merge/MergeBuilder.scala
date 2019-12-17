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

package swaydb.core.segment.merge

import swaydb.core.data
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.{Bytes, Options}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.mutable.ListBuffer
import scala.util.Random

sealed trait MergeBuilder extends Iterable[swaydb.core.data.Memory] {
  def keyValues: ListBuffer[swaydb.core.data.Memory]
  def addOne(keyValue: swaydb.core.data.Memory)

  def clear(): Unit =
    keyValues.clear()

  override def size: Int = keyValues.size

  override def iterator: Iterator[swaydb.core.data.Memory] =
    keyValues.iterator
}

object MergeBuilder {

  def random(keyValues: Iterable[swaydb.core.data.Memory]): MergeBuilder = {
    val builder = MergeBuilder.random()
    keyValues foreach builder.addOne
    builder
  }

  def random(): MergeBuilder =
    if (Random.nextBoolean())
      persistent()
    else if (Random.nextBoolean())
      memory()
    else
      buffer()

  def persistent(keyValues: Iterable[swaydb.core.data.Memory]): MergeBuilder.Persistent = {
    val builder = MergeBuilder.persistent()
    keyValues foreach builder.addOne
    builder
  }

  def persistent(): MergeBuilder.Persistent =
    new Persistent(
      maxMergedKeySize = 0,
      totalMergedKeysSize = 0,
      maxTimeSize = 0,
      totalTimesSize = 0,
      maxValueSize = 0,
      totalValuesSize = 0,
      totalDeadlineKeyValues = 0,
      totalRangeCount = 0,
      totalValuesCount = 0,
      hasRange = false,
      hasRemoveRange = false,
      hasPut = false,
      keyValues = ListBuffer.empty
    )

  def memory(keyValues: Iterable[swaydb.core.data.Memory]): MergeBuilder.Memory = {
    val builder = MergeBuilder.memory()
    keyValues foreach builder.addOne
    builder
  }

  def memory(): MergeBuilder.Memory =
    new Memory(
      segmentSize = 0,
      hasRange = false,
      hasPut = false,
      keyValues = ListBuffer.empty
    )

  def buffer(keyValues: Iterable[swaydb.core.data.Memory]): MergeBuilder.Buffer = {
    val builder = MergeBuilder.buffer()
    keyValues foreach builder.addOne
    builder
  }

  def buffer(): MergeBuilder.Buffer =
    new Buffer(ListBuffer.empty)

  class Persistent(var maxMergedKeySize: Int,
                   var totalMergedKeysSize: Int,
                   var maxTimeSize: Int,
                   var totalTimesSize: Int,
                   var maxValueSize: Int,
                   var totalValuesSize: Int,
                   var totalValuesCount: Int,
                   var totalDeadlineKeyValues: Int,
                   var totalRangeCount: Int,
                   var hasRange: Boolean,
                   var hasRemoveRange: Boolean,
                   var hasPut: Boolean,
                   val keyValues: ListBuffer[swaydb.core.data.Memory]) extends MergeBuilder {

    def hasDeadline = totalDeadlineKeyValues > 0

    /**
     * Format - keySize|key|keyValueId|accessIndex?|deadline|valueOffset|valueLength|time
     */
    def maxSortedIndexSize(hasAccessPositionIndex: Boolean): Int =
      (Bytes.sizeOfUnsignedInt(maxMergedKeySize) * keyValues.size) +
        totalMergedKeysSize +
        (if (hasAccessPositionIndex) Bytes.sizeOfUnsignedInt(keyValues.size) * keyValues.size else 0) +
        (KeyValueId.maxKeyValueIdByteSize * keyValues.size) + //keyValueId
        totalDeadlineKeyValues * ByteSizeOf.varLong + //deadline
        (Bytes.sizeOfUnsignedInt(totalValuesSize) * totalValuesCount) + //valueOffset
        (Bytes.sizeOfUnsignedInt(maxValueSize) * totalValuesCount) + //valueLength
        totalTimesSize

    def addOne(keyValue: swaydb.core.data.Memory) = {
      maxMergedKeySize = this.maxMergedKeySize max keyValue.mergedKey.size
      totalMergedKeysSize = this.totalMergedKeysSize + keyValue.mergedKey.size

      val persistentTimeSize = keyValue.persistentTime.size
      val timeSize = Bytes.sizeOfUnsignedInt(persistentTimeSize) + persistentTimeSize
      maxTimeSize = this.maxTimeSize max timeSize
      totalTimesSize = this.totalTimesSize + timeSize

      val valueSize = Options.valueOrElse[Slice[Byte], Int](keyValue.value, _.size, 0)
      maxValueSize = this.maxValueSize max valueSize
      totalValuesSize = this.totalValuesSize + valueSize

      if (keyValue.value.exists(_.nonEmpty))
        totalValuesCount += 1

      if (keyValue.deadline.isDefined)
        totalDeadlineKeyValues = this.totalDeadlineKeyValues + 1

      if (keyValue.isRange) {
        this.hasRange = true
        if (keyValue.isRemoveRangeMayBe)
          this.hasRemoveRange = true
        this.totalRangeCount = this.totalRangeCount + 1
      } else if (keyValue.isPut) {
        this.hasPut = true
      }

      keyValues addOne keyValue
    }
  }

  class Memory(var segmentSize: Int,
               var hasRange: Boolean,
               var hasPut: Boolean,
               val keyValues: ListBuffer[swaydb.core.data.Memory]) extends MergeBuilder {
    override def addOne(keyValue: data.Memory): Unit = {
      segmentSize = segmentSize + keyValue.key.size + Options.valueOrElse[Slice[Byte], Int](keyValue.value, _.size, 0)

      if (keyValue.isRange)
        this.hasRange = true
      else if (keyValue.isPut)
        this.hasPut = true

      keyValues addOne keyValue
    }
  }

  class Buffer(val keyValues: ListBuffer[swaydb.core.data.Memory]) extends MergeBuilder {
    override def addOne(keyValue: data.Memory): Unit =
      keyValues addOne keyValue
  }
}
