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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.bloomfilter

import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[core] object RangeFilter {

  case class State(uncommonBytesToTake: Int,
                   filters: mutable.Map[Int, Iterable[(Slice[Byte], Slice[Byte])]])

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
