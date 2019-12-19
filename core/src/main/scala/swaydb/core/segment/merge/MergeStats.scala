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

import swaydb.CollectionBuilder
import swaydb.core.data
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.util.ByteSizeOf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

private[core] sealed trait MergeStats[+T[_]] extends CollectionBuilder[data.Memory, T[data.Memory]] {

  def add(keyValue: data.Memory): Unit

  def clear(): Unit

  def result: T[data.Memory]
}

private[core] object MergeStats {

  def randomFrom(keyValues: Iterable[swaydb.core.data.Memory]): MergeStats[ListBuffer] =
    if (Random.nextBoolean())
      persistentFrom(keyValues)
    else if (Random.nextBoolean())
      memoryFrom(keyValues)
    else
      bufferFrom(keyValues)

  def random(): MergeStats[ListBuffer] =
    if (Random.nextBoolean())
      persistent(ListBuffer.newBuilder)
    else if (Random.nextBoolean())
      memory(ListBuffer.newBuilder)
    else
      buffer(ListBuffer.newBuilder)

  def persistentFrom(keyValues: Iterable[swaydb.core.data.Memory]): MergeStats.Persistent[ListBuffer] = {
    val stats = persistent(ListBuffer.newBuilder)
    keyValues foreach stats.add
    stats
  }

  def memoryFrom(keyValues: Iterable[swaydb.core.data.Memory]): MergeStats.Memory[ListBuffer] = {
    val stats = memory(ListBuffer.newBuilder)
    keyValues foreach stats.add
    stats
  }

  def bufferFrom(keyValues: Iterable[swaydb.core.data.Memory]): MergeStats.Buffer[ListBuffer] = {
    val stats = buffer(ListBuffer.newBuilder)
    keyValues foreach stats.add
    stats
  }

  def persistent[T[_]](builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]): MergeStats.Persistent[T] =
    new Persistent(
      maxMergedKeySize = 0,
      totalMergedKeysSize = 0,
      maxTimeSize = 0,
      totalTimesSize = 0,
      maxValueSize = 0,
      totalKeyValueCount = 0,
      totalValuesSize = 0,
      totalValuesCount = 0,
      totalDeadlineKeyValues = 0,
      totalRangeCount = 0,
      hasRange = false,
      hasRemoveRange = false,
      hasPut = false,
      builder = builder
    )

  def memory[T[_]](builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]): MergeStats.Memory[T] =
    new Memory(
      segmentSize = 0,
      hasRange = false,
      hasPut = false,
      builder = builder
    )

  def buffer[T[_]](builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]): MergeStats.Buffer[T] =
    new Buffer(builder)

  class Persistent[+T[_]](var maxMergedKeySize: Int,
                          var totalMergedKeysSize: Int,
                          var maxTimeSize: Int,
                          var totalTimesSize: Int,
                          var maxValueSize: Int,
                          var totalKeyValueCount: Int,
                          var totalValuesSize: Int,
                          var totalValuesCount: Int,
                          var totalDeadlineKeyValues: Int,
                          var totalRangeCount: Int,
                          var hasRange: Boolean,
                          var hasRemoveRange: Boolean,
                          var hasPut: Boolean,
                          builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]) extends MergeStats[T] {

    def hasDeadline = totalDeadlineKeyValues > 0

    def size = totalKeyValueCount

    def isEmpty: Boolean =
      size == 0

    def result: T[data.Memory] =
      builder.result()

    override def clear(): Unit =
      builder.clear()

    /**
     * Format - keySize|key|keyValueId|accessIndex?|deadline|valueOffset|valueLength|time
     */
    def maxSortedIndexSize(hasAccessPositionIndex: Boolean): Int =
      (Bytes.sizeOfUnsignedInt(maxMergedKeySize) * totalKeyValueCount) +
        totalMergedKeysSize +
        (if (hasAccessPositionIndex) Bytes.sizeOfUnsignedInt(totalKeyValueCount) * totalKeyValueCount else 0) +
        (KeyValueId.maxKeyValueIdByteSize * totalKeyValueCount) + //keyValueId
        totalDeadlineKeyValues * ByteSizeOf.varLong + //deadline
        (Bytes.sizeOfUnsignedInt(totalValuesSize) * totalValuesCount) + //valueOffset
        (Bytes.sizeOfUnsignedInt(maxValueSize) * totalValuesCount) + //valueLength
        totalTimesSize

    def updateStats(keyValue: swaydb.core.data.Memory) = {
      maxMergedKeySize = this.maxMergedKeySize max keyValue.mergedKey.size
      totalMergedKeysSize = this.totalMergedKeysSize + keyValue.mergedKey.size

      val persistentTimeSize = keyValue.persistentTime.size
      val timeSize = Bytes.sizeOfUnsignedInt(persistentTimeSize) + persistentTimeSize
      maxTimeSize = this.maxTimeSize max timeSize
      totalTimesSize = this.totalTimesSize + timeSize

      val valueSize = if (keyValue.value.isDefined) keyValue.value.get.size else 0
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
    }

    def add(keyValue: swaydb.core.data.Memory) = {
      totalKeyValueCount += 1
      updateStats(keyValue)
      builder += keyValue
    }

  }

  class Memory[+T[_]](var segmentSize: Int,
                      var hasRange: Boolean,
                      var hasPut: Boolean,
                      builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]) extends MergeStats[T] {
    override def result: T[data.Memory] =
      builder.result()

    override def clear(): Unit =
      builder.clear()

    override def add(keyValue: data.Memory): Unit = {
      segmentSize = segmentSize + keyValue.key.size + {
        if (keyValue.value.isDefined)
          keyValue.value.get.size
        else
          0
      }

      if (keyValue.isRange)
        this.hasRange = true
      else if (keyValue.isPut)
        this.hasPut = true

      builder += keyValue
    }
  }

  class Buffer[+T[_]](val builder: mutable.Builder[swaydb.core.data.Memory, T[swaydb.core.data.Memory]]) extends MergeStats[T] {
    override def add(keyValue: data.Memory): Unit =
      builder += keyValue

    override def clear(): Unit =
      builder.clear()

    override def result: T[data.Memory] =
      builder.result()
  }
}
