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

package swaydb.core.segment.format.a.block

import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[core] object RangeFilter {

  case class State(uncommonBytesToTake: Int,
                   filters: mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]])

  val emptyImmutable =
    new mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]] {
      override def +=(kv: (Int, Iterable[(Slice[Byte], Slice[Byte])])): this.type = throw new NotImplementedError("emptyRangeFilter")
      override def -=(key: Int): this.type = throw new NotImplementedError("emptyRangeFilter")
      override def get(key: Int): Option[Iterable[(Slice[Byte], Slice[Byte])]] = None
      override val iterator: Iterator[(Int, Iterable[(Slice[Byte], Slice[Byte])])] = Iterator.empty
    }

  def createState(uncommonBytesToStore: Int): Option[RangeFilter.State] =
    if (uncommonBytesToStore == 0)
      None
    else
      Some(
        State(
          uncommonBytesToTake = uncommonBytesToStore,
          filters = mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]
        )
      )

  def optimalRangeFilterByteSize(numberOfRanges: Int,
                                 uncommonBytesToStore: Int,
                                 preComputedRangeFilterCommonPrefixesCount: Iterable[Int]): Int =
    if (uncommonBytesToStore <= 0)
      1 //to store empty size.
    else
      IntMapListBufferSerializer.optimalBytesRequired(
        numberOfRanges = numberOfRanges,
        rangeFilterCommonPrefixes = preComputedRangeFilterCommonPrefixesCount,
        maxUncommonBytesToStore = uncommonBytesToStore
      )

  def add(from: Slice[Byte],
          to: Slice[Byte],
          state: RangeFilter.State): Unit = {
    val commonBytes = Bytes.commonPrefixBytes(from, to)
    val leftUncommonBytes = from.take(from.size - 1, state.uncommonBytesToTake)
    val rightUncommonBytes = to.take(to.size - 1, state.uncommonBytesToTake)
    val uncommonBytes: (Slice[Byte], Slice[Byte]) = (leftUncommonBytes, rightUncommonBytes)
    state.filters.get(commonBytes.size) map {
      ranges =>
        ranges.asInstanceOf[ListBuffer[(Slice[Byte], Slice[Byte])]] += uncommonBytes
    } getOrElse {
      state.filters.put(commonBytes.size, ListBuffer(uncommonBytes))
    }
  }

  def mightContain(key: Slice[Byte],
                   rangeFilterState: RangeFilter.State)(implicit ordering: KeyOrder[Slice[Byte]]): Boolean = {
    import ordering._
    rangeFilterState.filters exists {
      case (commonLowerBytes, rangeBytes) =>
        //todo - binary search.
        rangeBytes exists {
          case (leftBloomFilterRangeByte, rightBloomFilterRangeByte) =>
            val inputKeyWithCommonBytes = key take commonLowerBytes
            val inputKeyWithCommonAndMaxUncommonBytes = key take (commonLowerBytes + rangeFilterState.uncommonBytesToTake)
            val leftBytes = inputKeyWithCommonBytes ++ leftBloomFilterRangeByte
            val rightBytes = inputKeyWithCommonBytes ++ rightBloomFilterRangeByte
            leftBytes <= inputKeyWithCommonAndMaxUncommonBytes && inputKeyWithCommonAndMaxUncommonBytes < rightBytes
        }
    }
  }

  def find(key: Slice[Byte],
           rangeFilterState: RangeFilter.State,
           hashIndex: HashIndexBlock)(implicit ordering: KeyOrder[Slice[Byte]]): Option[Slice[Byte]] = {
    import ordering._
    //todo - binary search.
    rangeFilterState.filters foreach {
      case (commonLowerBytes, rangeBytes) =>
        var found: Slice[Byte] = null
        rangeBytes exists {
          case (leftBloomFilterRangeByte, rightBloomFilterRangeByte) =>
            val inputKeyWithCommonBytes = key take commonLowerBytes
            val inputKeyWithCommonAndMaxUncommonBytes = key take (commonLowerBytes + rangeFilterState.uncommonBytesToTake)
            val leftBytes = inputKeyWithCommonBytes ++ leftBloomFilterRangeByte
            val rightBytes = inputKeyWithCommonBytes ++ rightBloomFilterRangeByte
            if (leftBytes <= inputKeyWithCommonAndMaxUncommonBytes && inputKeyWithCommonAndMaxUncommonBytes < rightBytes) {
              found = if (leftBytes.size >= rightBytes.size) leftBytes else rightBytes
              true
            } else {
              false
            }
        }
        if (found != null)
          return Some(found)
    }
    None
  }
}
