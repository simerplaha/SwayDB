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

package swaydb.core.log

import swaydb.core.data.Memory
import swaydb.core.log.LogEntry.{Put, Remove}
import swaydb.core.log.serializer.{LogEntrySerialiser, LogEntryWriter}
import swaydb.core.segment.Segment
import swaydb.skiplist.{SkipList, SkipListBatchable, SkipListConcurrent}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceMut}

import scala.collection.mutable.ListBuffer

/**
 * [[LogEntry]]s can be batched via ++ function.
 *
 * Batched MapEntries are checksum and persisted as one batch operation.
 *
 * Batched MapEntries mutable (ListBuffer) to speed up boot-up time for Map recovery.
 * It should be changed to immutable List. Need to fix this.
 *
 * @tparam K Key type
 * @tparam V Value type
 */
private[swaydb] sealed trait LogEntry[K, +V] { thisEntry =>

  def applyBatch[T >: V](skipList: SkipListBatchable[_, _, K, T]): Unit

  def hasRange: Boolean
  def hasUpdate: Boolean
  def hasRemoveDeadline: Boolean

  def entriesCount: Int

  /**
   * Each map entry computes the bytes required for the entry on creation.
   * The total of all _entries are added to compute the file size of the Byte array to be persisted.
   *
   * This ensures that only single iteration will be required to create the final Byte array.
   */
  def entryBytesSize: Int

  def totalByteSize: Int =
    entryBytesSize + LogEntrySerialiser.headerSize

  def writeTo(slice: SliceMut[Byte]): Unit

  protected val _entries: ListBuffer[LogEntry.Point[K, _]]

  def asString(keyParser: K => String, valueParser: V => String): String = {
    this match {
      case Put(key, value) =>
        s"""
           |Type         : Add
           |key          : ${keyParser(key)}
           |Value        : ${valueParser(value)}
       """.stripMargin

      case Remove(key) =>
        s"""
           |Type          : Remove
           |key           : ${keyParser(key)}
       """.stripMargin
    }
  }
}

private[swaydb] case object LogEntry {

  val noneSegment = Option.empty[LogEntry[Slice[Byte], Segment]]

  val emptyListMemory = List.empty[LogEntry[Slice[Byte], Memory]]

  /**
   * Returns a combined Entry with duplicates removed from oldEntry, favouring newer duplicate entries.
   */
  def distinct[V](newEntry: LogEntry[Slice[Byte], V],
                  oldEntry: LogEntry[Slice[Byte], V])(implicit keyOrder: KeyOrder[Slice[Byte]]): LogEntry[Slice[Byte], V] = {
    import keyOrder._

    val olderEntriesUnique =
      oldEntry.entries filterNot {
        case LogEntry.Put(oldKey, _) =>
          newEntry.entries.exists {
            case LogEntry.Put(newKey, _) =>
              newKey equiv oldKey

            case LogEntry.Remove(newKey) =>
              newKey equiv oldKey
          }

        case LogEntry.Remove(oldKey) =>
          newEntry.entries.exists {
            case LogEntry.Put(newKey, _) =>
              newKey equiv oldKey

            case LogEntry.Remove(newKey) =>
              newKey equiv oldKey
          }
      }

    olderEntriesUnique.foldLeft(newEntry) {
      case (newEntry, oldEntry) =>
        newEntry ++ {
          oldEntry match {
            case entry @ LogEntry.Put(_, _) =>
              entry.copySingle()

            case entry @ LogEntry.Remove(_) =>
              entry.copySingle()
          }
        }
    }
  }

  implicit class MapEntriesBatch[K, V](left: LogEntry[K, V]) {
    def ++(right: LogEntry[K, V]): LogEntry[K, V] =
      new Batch[K, V] {

        override protected val _entries: ListBuffer[LogEntry.Point[K, _]] =
          left._entries ++= right._entries

        override val entryBytesSize: Int =
          left.entryBytesSize + right.entryBytesSize

        override def writeTo(slice: SliceMut[Byte]): Unit =
          _entries foreach (_.writeTo(slice))

        override def asString(keyParser: K => String, valueParser: V => String): String =
          s"""${left.asString(keyParser, valueParser)}${right.asString(keyParser, valueParser)}"""

        //        override def applyTo[T >: V](skipList: ConcurrentSkipList[K, T]): Unit =
        //          _entries.asInstanceOf[ListBuffer[LogEntry[K, V]]] foreach (_.applyTo(skipList))

        override def applyBatch[T >: V](skipList: SkipListBatchable[_, _, K, T]): Unit = {
          val entries = _entries.asInstanceOf[ListBuffer[LogEntry.Point[K, V]]]

          if (entries.size <= 1)
            entries.headOption foreach {
              case LogEntry.Put(key, value) =>
                skipList.put(key, value)

              case LogEntry.Remove(key) =>
                skipList.remove(key)
            }
          else
            skipList batch {
              skipList: SkipListConcurrent[_, _, K, T] =>
                entries map {
                  case LogEntry.Put(key, value) =>
                    skipList.put(key, value)

                  case LogEntry.Remove(key) =>
                    skipList.remove(key)
                }
            }
        }

        override val hasRange: Boolean =
          left.hasRange || right.hasRange

        override val hasUpdate: Boolean =
          left.hasUpdate || right.hasUpdate

        override val hasRemoveDeadline: Boolean =
          left.hasRemoveDeadline || right.hasRemoveDeadline

        def entriesCount: Int =
          _entries.size
      }

    def entries: ListBuffer[Point[K, V]] =
      left._entries.asInstanceOf[ListBuffer[Point[K, V]]]
  }

  sealed trait Batch[K, +V] extends LogEntry[K, V]
  sealed trait Point[K, +V] extends LogEntry[K, V] {
    def key: K

    def applyPoint[T >: V](skipList: SkipList[_, _, K, T]): Unit
  }

  case class Put[K, V](key: K,
                       value: V)(implicit serializer: LogEntryWriter[LogEntry.Put[K, V]]) extends LogEntry.Point[K, V] {

    private var calculatedEntriesByteSize: Int = -1
    def hasRange: Boolean = serializer.isRange
    def hasUpdate: Boolean = serializer.isUpdate
    def hasRemoveDeadline: Boolean =
      value match {
        case Memory.Remove(_, Some(_), _) => true
        case _ => false
      }

    /**
     * This function can be called multiple time. Store the calculation internally to save compute.
     */
    override def entryBytesSize: Int = {
      if (calculatedEntriesByteSize == -1)
        calculatedEntriesByteSize = serializer bytesRequired this

      calculatedEntriesByteSize
    }

    override def writeTo(slice: SliceMut[Byte]): Unit =
      serializer.write(this, slice)

    override def applyBatch[T >: V](skipList: SkipListBatchable[_, _, K, T]): Unit =
      skipList.put(key, value)

    override def applyPoint[T >: V](skipList: SkipList[_, _, K, T]): Unit =
      skipList.put(key, value)

    def entriesCount: Int =
      1

    //copy single creates a new Map entry clearing the ListBuffer. Immutable list is used here to speed boot-up
    //times when recovery key-values from Level0's map files. This should be changed to be immutable.
    def copySingle() =
      copy()

    override protected val _entries: ListBuffer[LogEntry.Point[K, _]] = ListBuffer(this)

  }

  case class Remove[K](key: K)(implicit serializer: LogEntryWriter[LogEntry.Remove[K]]) extends LogEntry.Point[K, Nothing] {

    private var calculatedEntriesByteSize: Int = -1
    def hasRange: Boolean = serializer.isRange
    def hasUpdate: Boolean = serializer.isUpdate
    def hasRemoveDeadline: Boolean = false

    /**
     * This function can be called multiple time. Store the calculation internally to save compute.
     */
    override def entryBytesSize: Int = {
      if (calculatedEntriesByteSize == -1)
        calculatedEntriesByteSize = serializer bytesRequired this

      calculatedEntriesByteSize
    }

    override def writeTo(slice: SliceMut[Byte]): Unit =
      serializer.write(this, slice)

    override def applyBatch[T >: Nothing](skipList: SkipListBatchable[_, _, K, T]): Unit =
      skipList.remove(key)

    override def applyPoint[T >: Nothing](skipList: SkipList[_, _, K, T]): Unit =
      skipList.remove(key)

    def entriesCount: Int =
      1

    def copySingle() =
      copy()

    override protected val _entries: ListBuffer[LogEntry.Point[K, _]] = ListBuffer(this)
  }
}
