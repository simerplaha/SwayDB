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

import swaydb.{ErrorHandler, IO}
import swaydb.core.data.Transient
import swaydb.core.group.compression.data.GroupByInternal
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
 * A mutable Buffer that maintains the current state of Grouped key-values for a Segment.
 */
sealed trait SegmentBuffer extends Iterable[Transient] {
  def add(keyValue: Transient.SegmentResponse): Unit
  def lastOption: Option[Transient]
  def lastNonGroup: Transient.SegmentResponse
  def lastNonGroupOption: Option[Transient.SegmentResponse]
  def nonEmpty: Boolean
  def isEmpty: Boolean
  def size: Int
  def isReadyForGrouping: Boolean
}

object SegmentBuffer {

  def apply(groupBy: Option[GroupByInternal.KeyValues]): SegmentBuffer =
    groupBy match {
      case Some(groupBy) =>
        new Grouped(ListBuffer.empty, groupBy, Slice.create[Transient.SegmentResponse](groupBy.count))

      case None =>
        new Flattened(ListBuffer.empty[Transient.SegmentResponse])
    }

  def apply(groupBy: GroupByInternal.KeyValues): SegmentBuffer.Grouped =
    new Grouped(ListBuffer.empty, groupBy, Slice.create[Transient.SegmentResponse](groupBy.count))

  class Flattened(keyValues: ListBuffer[Transient.SegmentResponse]) extends SegmentBuffer {
    def add(keyValue: Transient.SegmentResponse): Unit =
      keyValues += keyValue

    override def lastNonGroup =
      keyValues.last

    override def lastNonGroupOption =
      keyValues.lastOption

    override def nonEmpty: Boolean =
      keyValues.nonEmpty

    override def isEmpty: Boolean =
      keyValues.isEmpty

    override def size: Int =
      keyValues.size

    override def lastOption: Option[Transient] =
      keyValues.lastOption

    override def last: Transient.SegmentResponse =
      keyValues.last

    override def head =
      keyValues.head

    override def headOption =
      keyValues.headOption

    override def iterator: Iterator[Transient] =
      keyValues.iterator

    override def isReadyForGrouping: Boolean =
      false
  }

  class Grouped(groups: ListBuffer[Transient.Group],
                val groupBy: GroupByInternal.KeyValues,
                private var _unGrouped: Slice[Transient.SegmentResponse]) extends SegmentBuffer {

    def groupedKeyValues: Iterable[Transient.Group] =
      groups

    def unGrouped =
      _unGrouped

    def add(keyValue: Transient.SegmentResponse): Unit =
      _unGrouped add keyValue

    def addGroup[T: ErrorHandler](keyValue: Transient.Group): IO[T, Unit] =
      if (_unGrouped.nonEmpty) {
        IO.failed("Cannot add group. Has unGrouped key-values.")
      } else {
        groups += keyValue
        IO.unit
      }

    def replaceGroupedKeyValues(keyValue: Transient.Group): Unit = {
      groups += keyValue
      _unGrouped = Slice.create[Transient.SegmentResponse](groupBy.count)
    }

    def replaceGroupedGroups(group: Transient.Group): Unit = {
      groups.clear()
      groups += group
    }

    def isLastGroup: Boolean =
      groups.nonEmpty && _unGrouped.isEmpty

    def currentGroups: Iterable[Transient.Group] =
      groups

    /**
     * The cost of updatePrevious will be negligible since the number of grouping
     * of groups is expected to be very low.
     */
    def getGroupsToGroup(groupBy: GroupByInternal.Groups): Slice[Transient.Group] = {
      val slicedGroups = Slice.create[Transient.Group](groups.size)
      groups foreach {
        group =>
          slicedGroups add
            group.updatePrevious(
              valuesConfig = groupBy.valuesConfig,
              sortedIndexConfig = groupBy.sortedIndexConfig,
              binarySearchIndexConfig = groupBy.binarySearchIndexConfig,
              hashIndexConfig = groupBy.hashIndexConfig,
              bloomFilterConfig = groupBy.bloomFilterConfig,
              previous = slicedGroups.lastOption
            )
      }
      slicedGroups
    }

    def shouldGroupKeyValues(force: Boolean): Boolean =
      _unGrouped.nonEmpty && {
        force ||
          _unGrouped.isFull ||
          _unGrouped.size >= groupBy.count || {
          groupBy.size exists {
            size =>
              _unGrouped.lastOption exists {
                last =>
                  last.stats.segmentSizeWithoutFooter >= size
              }
          }
        }
      }

    def shouldGroupGroups(): Boolean =
      groupBy.groupByGroups exists {
        groupBy =>
          groups.nonEmpty && {
            groups.size >= groupBy.count || {
              groupBy.size exists {
                size =>
                  groups.lastOption exists {
                    last =>
                      last.stats.segmentSizeWithoutFooter >= size
                  }
              }
            }
          }
      }

    override def isReadyForGrouping: Boolean =
      _unGrouped.isFull

    override def lastNonGroup: Transient.SegmentResponse =
      _unGrouped.last

    override def lastNonGroupOption: Option[Transient.SegmentResponse] =
      _unGrouped.lastOption

    def lastGroup: Option[Transient.Group] =
      groups.lastOption

    override def lastOption: Option[Transient] =
      _unGrouped.lastOption orElse groups.lastOption

    override def nonEmpty: Boolean =
      groups.nonEmpty || _unGrouped.nonEmpty

    override def isEmpty: Boolean =
      groups.isEmpty && _unGrouped.nonEmpty

    override def size: Int =
      groups.size + _unGrouped.size

    override def head =
      unGrouped.headOption getOrElse groups.head

    override def headOption =
      unGrouped.headOption orElse groups.headOption

    override def iterator: Iterator[Transient] =
      new Iterator[Transient] {
        val left = groups.iterator
        val right = _unGrouped.iterator

        var nextOne: Transient = _

        override def hasNext: Boolean =
          if (left.hasNext) {
            nextOne = left.next()
            true
          } else if (right.hasNext) {
            nextOne = right.next()
            true
          } else {
            nextOne = null
            false
          }

        override def next(): Transient =
          nextOne
      }
  }
}
