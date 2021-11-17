/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.merge.stats

import swaydb.utils.Aggregator
import swaydb.core.segment.data
import swaydb.core.segment.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.utils.ByteSizeOf

import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer

/**
 * Instance that accumulates merge statistics, maximum fileSize etc as merge progresses.
 */
private[core] sealed trait MergeStats[-FROM, +T[_]] extends Aggregator[FROM, T[data.Memory]] {

  def addOne(keyValue: FROM): this.type

  override def addAll(items: IterableOnce[FROM]): MergeStats.this.type = {
    items foreach addOne
    this
  }

  def keyValues: T[data.Memory]

  override def result: T[data.Memory] =
    keyValues
}

private[core] case object MergeStats {

  private[core] sealed trait Segment[-FROM, +T[_]] extends MergeStats[FROM, T] {

    def isEmpty: Boolean

    def keyValues: T[data.Memory]

  }

  sealed trait ClosedStats[+T[_]] {
    def isEmpty: Boolean
    def keyValuesCount: Int
    def keyValues: T[data.Memory]
  }

  implicit val memoryToMemory: data.Memory => data.Memory =
    (memory: data.Memory) => memory

  def persistentBuilder[FROM](keyValues: IterableOnce[FROM])(implicit convert: FROM => data.Memory): MergeStats.Persistent.Builder[FROM, ListBuffer] =
    persistent[FROM, ListBuffer](Aggregator.listBuffer) addAll keyValues

  def memoryBuilder[FROM](keyValues: IterableOnce[FROM])(implicit convert: FROM => data.Memory): MergeStats.Memory.Builder[FROM, ListBuffer] =
    memory[FROM, ListBuffer](Aggregator.listBuffer) addAll keyValues

  def bufferBuilder[FROM](keyValues: IterableOnce[FROM])(implicit convert: FROM => data.Memory): MergeStats.Buffer[FROM, ListBuffer] =
    buffer[FROM, ListBuffer](Aggregator.listBuffer) addAll keyValues

  def persistent[FROM, T[_]](aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory): MergeStats.Persistent.Builder[FROM, T] =
    new Persistent.Builder(
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
      mightContainRemoveRange = false,
      hasPut = false,
      aggregator = aggregator
    )

  def memory[FROM, T[_]](aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory): MergeStats.Memory.Builder[FROM, T] =
    new Memory.Builder[FROM, T](
      aggregator = aggregator
    )

  def buffer[FROM, T[_]](aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory): MergeStats.Buffer[FROM, T] =
    new Buffer(aggregator)

  object Persistent {
    class Builder[-FROM, +T[_]](var maxMergedKeySize: Int,
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
                                var mightContainRemoveRange: Boolean,
                                var hasPut: Boolean,
                                aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory) extends MergeStats.Segment[FROM, T] {

      def close(hasAccessPositionIndex: Boolean, optimiseForReverseIteration: Boolean): MergeStats.Persistent.Closed[T] =
        new MergeStats.Persistent.Closed[T](
          isEmpty = this.isEmpty,
          keyValuesCount = size,
          keyValues = keyValues,
          totalValuesSize = totalValuesSize,
          maxSortedIndexSize =
            maxSortedIndexSize(
              hasAccessPositionIndex = hasAccessPositionIndex,
              optimiseForReverseIteration = optimiseForReverseIteration
            )
        )

      def hasDeadline = totalDeadlineKeyValues > 0

      def size = totalKeyValueCount

      def isEmpty: Boolean =
        size == 0

      def keyValues: T[data.Memory] =
        aggregator.result

      /**
       * Format - keySize|key|keyValueId|accessIndex?|previousIndexOffset?|deadline|valueOffset|valueLength|time
       */
      def maxSortedIndexSize(hasAccessPositionIndex: Boolean, optimiseForReverseIteration: Boolean): Int = {
        val maxSize =
          (Bytes.sizeOfUnsignedInt(maxMergedKeySize) * totalKeyValueCount) +
            totalMergedKeysSize +
            (if (hasAccessPositionIndex) Bytes.sizeOfUnsignedInt(totalKeyValueCount) * totalKeyValueCount else 0) +
            (KeyValueId.maxKeyValueIdByteSize * totalKeyValueCount) + //keyValueId
            totalDeadlineKeyValues * ByteSizeOf.varLong + //deadline
            (Bytes.sizeOfUnsignedInt(totalValuesSize) * totalValuesCount) + //valueOffset
            (Bytes.sizeOfUnsignedInt(maxValueSize) * totalValuesCount) + //valueLength
            totalTimesSize

        if (optimiseForReverseIteration)
          maxSize + (Bytes.sizeOfUnsignedInt(maxSize) * totalKeyValueCount)
        else
          maxSize
      }

      def updateStats(keyValue: data.Memory): Unit = {
        maxMergedKeySize = this.maxMergedKeySize max keyValue.mergedKey.size
        totalMergedKeysSize = this.totalMergedKeysSize + keyValue.mergedKey.size

        val persistentTimeSize = keyValue.persistentTime.size
        val timeSize = Bytes.sizeOfUnsignedInt(persistentTimeSize) + persistentTimeSize
        maxTimeSize = this.maxTimeSize max timeSize
        totalTimesSize = this.totalTimesSize + timeSize

        val valueSize = if (keyValue.value.isSomeC) keyValue.value.getC.size else 0
        maxValueSize = this.maxValueSize max valueSize
        totalValuesSize = this.totalValuesSize + valueSize

        if (keyValue.value.existsC(_.nonEmpty))
          totalValuesCount += 1

        if (keyValue.deadline.isDefined)
          totalDeadlineKeyValues = this.totalDeadlineKeyValues + 1

        if (keyValue.isRange) {
          this.hasRange = true
          if (keyValue.mightContainRemoveRange)
            this.mightContainRemoveRange = true
          this.totalRangeCount = this.totalRangeCount + 1
        } else if (keyValue.isPut) {
          this.hasPut = true
        }
      }

      def addOne(from: FROM): this.type = {
        val keyValueOrNull = converterOrNull(from)

        if (keyValueOrNull != null) {
          totalKeyValueCount += 1
          updateStats(keyValueOrNull)
          aggregator addOne keyValueOrNull
        }

        this
      }
    }

    /**
     * Does not expose [[Closed.keyValues]] to handle
     * cases where the collection type T[_] type might
     * be [[IterableOnce]]
     */
    sealed trait ClosedStatsOnly {
      def isEmpty: Boolean
      def keyValuesCount: Int
      def maxSortedIndexSize: Int
      def totalValuesSize: Int
    }

    class Closed[+T[_]](val isEmpty: Boolean,
                        val keyValuesCount: Int,
                        val keyValues: T[data.Memory],
                        val maxSortedIndexSize: Int,
                        val totalValuesSize: Int) extends ClosedStats[T] with ClosedStatsOnly
  }

  object Memory {
    def calculateSize(keyValue: data.Memory): Int =
      keyValue.key.size + {
        if (keyValue.value.isSomeC)
          keyValue.value.getC.size
        else
          0
      }

    class Builder[-FROM, +T[_]](aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory) extends MergeStats.Segment[FROM, T] {

      private var _segmentSize = 0
      private var _totalKeyValueCount: Int = 0

      override def isEmpty =
        _totalKeyValueCount == 0

      def segmentSize: Int =
        _segmentSize

      def keyValueCount: Int =
        _totalKeyValueCount

      override def keyValues: T[data.Memory] =
        aggregator.result

      override def addOne(from: FROM): this.type = {
        val keyValueOrNull = converterOrNull(from)

        if (keyValueOrNull != null) {
          aggregator addOne keyValueOrNull
          _totalKeyValueCount += 1
          _segmentSize += MergeStats.Memory calculateSize keyValueOrNull
        }

        this
      }

      def close: Memory.Closed[T] =
        new Closed[T](
          isEmpty = isEmpty,
          keyValues = keyValues,
          keyValuesCount = _totalKeyValueCount,
          segmentSize = _segmentSize
        )
    }

    /**
     * Similar to [[ClosedStats]] but does not require
     */
    class ClosedIgnoreStats[+T[_]](val isEmpty: Boolean,
                                   val keyValues: T[data.Memory])

    class Closed[+T[_]](override val isEmpty: Boolean,
                        override val keyValues: T[data.Memory],
                        val keyValuesCount: Int,
                        val segmentSize: Int) extends ClosedIgnoreStats[T](isEmpty, keyValues) with ClosedStats[T]
  }

  class Buffer[-FROM, +T[_]](aggregator: Aggregator[swaydb.core.segment.data.Memory, T[swaydb.core.segment.data.Memory]])(implicit converterOrNull: FROM => data.Memory) extends MergeStats[FROM, T] {

    override def addOne(from: FROM): this.type = {
      val keyValueOrNull = converterOrNull(from)

      if (keyValueOrNull != null)
        aggregator addOne keyValueOrNull

      this
    }

    override def keyValues: T[data.Memory] =
      aggregator.result
  }
}
