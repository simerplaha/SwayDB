/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.map

import swaydb.core.data.Memory
import swaydb.core.map.MapEntry.{Put, Remove}
import swaydb.core.map.serializer.{MapCodec, MapEntryWriter}
import swaydb.core.util.skiplist.{SkipList, SkipListBatchable, SkipListConcurrent}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
 * [[MapEntry]]s can be batched via ++ function.
 *
 * Batched MapEntries are checksum and persisted as one batch operation.
 *
 * Batched MapEntries mutable (ListBuffer) to speed up boot-up time for Map recovery.
 * It should be changed to immutable List. Need to fix this.
 *
 * @tparam K Key type
 * @tparam V Value type
 */
private[swaydb] sealed trait MapEntry[K, +V] { thisEntry =>

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
    entryBytesSize + MapCodec.headerSize

  def writeTo(slice: Slice[Byte]): Unit

  protected val _entries: ListBuffer[MapEntry.Point[K, _]]

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

private[swaydb] case object MapEntry {

  val emptyListMemory = List.empty[MapEntry[Slice[Byte], Memory]]

  /**
   * Returns a combined Entry with duplicates removed from oldEntry, favouring newer duplicate entries.
   */
  def distinct[V](newEntry: MapEntry[Slice[Byte], V],
                  oldEntry: MapEntry[Slice[Byte], V])(implicit keyOrder: KeyOrder[Slice[Byte]]): MapEntry[Slice[Byte], V] = {
    import keyOrder._

    val olderEntriesUnique =
      oldEntry.entries filterNot {
        case MapEntry.Put(oldKey, _) =>
          newEntry.entries.exists {
            case MapEntry.Put(newKey, _) =>
              newKey equiv oldKey

            case MapEntry.Remove(newKey) =>
              newKey equiv oldKey
          }

        case MapEntry.Remove(oldKey) =>
          newEntry.entries.exists {
            case MapEntry.Put(newKey, _) =>
              newKey equiv oldKey

            case MapEntry.Remove(newKey) =>
              newKey equiv oldKey
          }
      }

    olderEntriesUnique.foldLeft(newEntry) {
      case (newEntry, oldEntry) =>
        newEntry ++ {
          oldEntry match {
            case entry @ MapEntry.Put(_, _) =>
              entry.copySingle()

            case entry @ MapEntry.Remove(_) =>
              entry.copySingle()
          }
        }
    }
  }

  implicit class MapEntriesBatch[K, V](left: MapEntry[K, V]) {
    def ++(right: MapEntry[K, V]): MapEntry[K, V] =
      new Batch[K, V] {

        override protected val _entries: ListBuffer[MapEntry.Point[K, _]] =
          left._entries ++= right._entries

        override val entryBytesSize: Int =
          left.entryBytesSize + right.entryBytesSize

        override def writeTo(slice: Slice[Byte]): Unit =
          _entries foreach (_.writeTo(slice))

        override def asString(keyParser: K => String, valueParser: V => String): String =
          s"""${left.asString(keyParser, valueParser)}${right.asString(keyParser, valueParser)}"""

        //        override def applyTo[T >: V](skipList: ConcurrentSkipList[K, T]): Unit =
        //          _entries.asInstanceOf[ListBuffer[MapEntry[K, V]]] foreach (_.applyTo(skipList))

        override def applyBatch[T >: V](skipList: SkipListBatchable[_, _, K, T]): Unit = {
          val entries = _entries.asInstanceOf[ListBuffer[MapEntry.Point[K, V]]]

          if (entries.size <= 1)
            entries.headOption foreach {
              case MapEntry.Put(key, value) =>
                skipList.put(key, value)

              case MapEntry.Remove(key) =>
                skipList.remove(key)
            }
          else
            skipList batch {
              skipList: SkipListConcurrent[_, _, K, T] =>
                entries map {
                  case MapEntry.Put(key, value) =>
                    skipList.put(key, value)

                  case MapEntry.Remove(key) =>
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

  sealed trait Batch[K, +V] extends MapEntry[K, V]
  sealed trait Point[K, +V] extends MapEntry[K, V] {
    def key: K

    def applyPoint[T >: V](skipList: SkipList[_, _, K, T]): Unit
  }

  case class Put[K, V](key: K,
                       value: V)(implicit serializer: MapEntryWriter[MapEntry.Put[K, V]]) extends MapEntry.Point[K, V] {

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

    override def writeTo(slice: Slice[Byte]): Unit =
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

    override protected val _entries: ListBuffer[MapEntry.Point[K, _]] = ListBuffer(this)

  }

  case class Remove[K](key: K)(implicit serializer: MapEntryWriter[MapEntry.Remove[K]]) extends MapEntry.Point[K, Nothing] {

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

    override def writeTo(slice: Slice[Byte]): Unit =
      serializer.write(this, slice)

    override def applyBatch[T >: Nothing](skipList: SkipListBatchable[_, _, K, T]): Unit =
      skipList.remove(key)

    override def applyPoint[T >: Nothing](skipList: SkipList[_, _, K, T]): Unit =
      skipList.remove(key)

    def entriesCount: Int =
      1

    def copySingle() =
      copy()

    override protected val _entries: ListBuffer[MapEntry.Point[K, _]] = ListBuffer(this)
  }
}
