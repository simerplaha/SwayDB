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

package swaydb.core.segment.block.bloomfilter

import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

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

  def optimalRangeFilterByteSize(rangeCount: Int,
                                 uncommonBytesToStore: Int,
                                 preComputedRangeFilterCommonPrefixesCount: Iterable[Int]): Int =
    if (uncommonBytesToStore <= 0)
      1 //to store empty size.
    else
      IntMapListBufferSerializer.optimalBytesRequired(
        rangeCount = rangeCount,
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
