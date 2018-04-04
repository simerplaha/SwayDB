/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.level.zero

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.data.{Memory, Value}
import swaydb.core.map.{MapEntry, SkipListMerge}
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._

object LevelZeroSkipListMerge extends SkipListMerge[Slice[Byte], Memory] {

  /**
    * Pre-requisite: splitKey should always be within range's fromKey and less than toKey.
    */
  private def split(splitKey: Slice[Byte],
                    splitFromValue: Option[Value],
                    range: Memory.Range)(implicit ordering: Ordering[Slice[Byte]]): (Memory.Range, Option[Memory.Range]) = {
    import ordering._
    if (splitKey equiv range.fromKey) //if the splitKey == range's from key, update the fromKeyValue in the range.
      (
        Memory.Range(range.fromKey, range.toKey, splitFromValue orElse range.fromValue, range.rangeValue),
        None
      )
    else
      (
        Memory.Range(range.fromKey, splitKey, range.fromValue, range.rangeValue),
        Some(Memory.Range(splitKey, range.toKey, splitFromValue, range.rangeValue))
      )
  }

  //pre-requisite: split keys should always be greater than range's fromKey
  //3'rd range is returned None if the splitToKey > the range's toKey.
  private def split(splitFromKey: Slice[Byte],
                    splitToKey: Slice[Byte],
                    splitFromValue: Option[Value],
                    range: Memory.Range)(implicit ordering: Ordering[Slice[Byte]]): (Memory.Range, Option[Memory.Range], Option[Memory.Range]) = {
    import ordering._
    split(splitFromKey, splitFromValue, range) match {
      case (left, Some(right)) =>
        if (splitToKey >= right.toKey) {
          (left, Some(right), None)
        } else {
          val (mid, rightRight) = split(splitToKey, None, right)
          (left, Some(mid), rightRight)
        }

      case (left, None) =>
        if (splitToKey >= left.toKey) {
          (left, None, None)
        } else {
          val (leftLeft, mid) = split(splitToKey, None, left)
          (leftLeft, mid, None)
        }
    }
  }

  //split if the edge is a Range.
  private def adjustEdge(splitKey: Slice[Byte],
                         edge: Memory,
                         skipList: ConcurrentSkipListMap[Slice[Byte], Memory])(implicit ordering: Ordering[Slice[Byte]]) = {
    import ordering._
    edge match {
      case edgeRange: Memory.Range if splitKey > edgeRange.fromKey && splitKey < edgeRange.toKey => //adjust only if within the range
        split(splitKey, None, edgeRange) match {
          case ((_, None)) =>
          //if only 1 split is returned then the
          //head fully overlaps the insert range's toKey.
          //no change required, the submission process will fix this ranges.
          case ((lowerRange, Some(upperRange))) =>
            //Split occurred, stash the lowerRange and keep the upperRange skipList.
            skipList.put(upperRange.fromKey, upperRange) //put the upperRange, this will not alter the previous state of the skipList as only new entry is being added.
            skipList.put(lowerRange.fromKey, lowerRange)
        }
      case _ =>
      //adjust not required
    }
  }

  /**
    * Inserts a [[Memory.Fixed]] key-value into skipList.
    */
  def insert(insert: Memory.Fixed,
             skipList: ConcurrentSkipListMap[Slice[Byte], Memory])(implicit ordering: Ordering[Slice[Byte]]): Unit = {
    import ordering._
    Option(skipList.floorEntry(insert.key)) match {
      case Some(floorEntry) =>
        floorEntry.getValue match {
          //if floor entry for input fixed entry, simply put to replace the old entry if the keys are the same or it will as a new entry.
          case _: Memory.Fixed =>
            skipList.put(insert.key, insert)

          //if the floor entry is a range try to do a split. split function might not return splits if the insertKey
          //is greater than range's toKey since toKeys' of Ranges are exclusive.
          case floorRange: Memory.Range if insert.key < floorRange.toKey => //if the fixed key is smaller than the range's toKey then do a split.
            val insertValue =
              insert match {
                case put: Memory.Put => Value.Put(put.value)
                case _: Memory.Remove => Value.Remove
              }
            split(insert.key, Some(insertValue), floorRange) match {
              case ((left, right)) =>
                right foreach (right => skipList.put(right.fromKey, right))
                skipList.put(left.fromKey, left)
            }
          case _ =>
            skipList.put(insert.key, insert)
        }

      //if there is no floor, simply put.
      case None =>
        skipList.put(insert.key, insert)
    }
  }

  /**
    * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
    * the skipList before applying the new state so that all read queries read the latest write.
    */
  def insert(insert: Memory.Range,
             skipList: ConcurrentSkipListMap[Slice[Byte], Memory])(implicit ordering: Ordering[Slice[Byte]]): Unit = {
    import ordering._
    //get the start position of this range to fetch the range's start and end key-values for the skipList.
    val insertRangesFloorKey = Option(skipList.floorEntry(insert.fromKey)) map {
      floorEntry =>
        floorEntry.getValue match {
          case _: Memory.Fixed if floorEntry.getKey >= insert.fromKey =>
            floorEntry.getKey

          case range: Memory.Range if insert.fromKey < range.toKey =>
            floorEntry.getKey

          case _ =>
            insert.fromKey
        }
    } getOrElse insert.fromKey
    //fetch the key-values that fall within the range and submit all the key's that have fixed value set
    //to the new range and split the edge ranges that do not fall within the range.
    val conflictingRangeKeyValues = skipList.subMap(insertRangesFloorKey, true, insert.toKey, false)

    if (conflictingRangeKeyValues.isEmpty) {
      skipList.put(insert.fromKey, insert)
    } else {
      //fix left edge.
      Option(conflictingRangeKeyValues.firstEntry()) foreach {
        left =>
          adjustEdge(insert.fromKey, left.getValue, skipList)
      }

      //fix right edge.
      Option(conflictingRangeKeyValues.lastEntry()) foreach {
        right =>
          adjustEdge(insert.toKey, right.getValue, skipList)
      }

      //start submitting maps fixed key-values to the new inserted range to re-adjust the ranges.
      //the skip list now contains only the key-values that belong to the range. stashed edge key-values can be
      //insert in the end.
      //start from the inserted key-value and iteratively submit Value.fixed or range's fromKeyValue: Fixed to the range's last split.
      //left and right are adjust, re-subMap to get key-values that fall within the range.

      //      def asString(value: Value): String =
      //        value match {
      //          case value: Fixed =>
      //            value match {
      //              case _: Value.Remove =>
      //                "Remove"
      //              case Value.Put(value) =>
      //                s"Put(${value.map(_.readInt()).getOrElse("None")})"
      //            }
      //          case Value.Range(toKey, fromValue, rangeValue) =>
      //            s"""Range(toKey = ${toKey.readInt()}, fromValue = ${fromValue.map(asString(_))}, rangeValue = ${asString(rangeValue)})"""
      //        }

      (insert.fromValue, insert.rangeValue) match {
        case (_, Value.Remove) =>
          skipList.put(insert.fromKey, insert)
          skipList.subMap(insert.fromKey, false, insert.toKey, false).clear()

        case (_, Value.Put(_)) =>
          skipList.subMap(insert.fromKey, true, insert.toKey, false)
            .asScala
            .foldLeft((insert, false)) { //Boolean indicates if the current remaining range is inserted. If it's not then it will be inserted at the end of the fold.
              case result @ ((last, lastInserted), conflicting) =>
                //                println(s"Current lastRange: ${lastRangeKey.readInt()} -> ${asString(lastRangeValue)}")
                //                println(s"Conflicting range: ${conflictingKey.readInt()} -> ${asString(conflictingValue)}")

                result match {
                  case ((nextInsertRange, _), (_, conflicting)) =>

                    conflicting match {
                      case conflicting: Memory.Fixed =>
                        val conflictingKeyValue =
                          nextInsertRange.fromValue match {
                            case Some(value) if conflicting.key equiv nextInsertRange.fromKey =>
                              value
                            case _ =>
                              if (conflicting.isRemove)
                                Value.Remove
                              else
                                insert.rangeValue
                          }

                        val (left, right) = split(conflicting.key, Some(conflictingKeyValue), nextInsertRange)
                        right foreach (right => skipList.put(right.fromKey, right))
                        skipList.put(left.fromKey, left)
                        (right getOrElse left, true)

                      //match if conflicting range was remove. Updates cannot be applied on removed ranges, removed ranges are kept as is.
                      case Memory.Range(conflictingFromKey, conflictingToKey, conflictingFromValue, Value.Remove) =>
                        //if fromValue is set then this is an actual key and should be updated.
                        val splitFromValue =
                          nextInsertRange.fromValue match {
                            case Some(value) if conflicting.key equiv nextInsertRange.fromKey =>
                              Some(value)
                            case _ =>
                              conflictingFromValue match {
                                case removed @ Some(Value.Remove) => removed //if the key was remove, Update will not be applied, it will stay removed.
                                case Some(_) => Some(insert.rangeValue) //if the key exists and was not remove, update will be applied to be the range's value.
                                case None => None //else the key does not exists so there is no split value.
                              }
                          }

                        split(conflictingFromKey, conflictingToKey, splitFromValue, nextInsertRange) match {
                          case (left, Some(mid), Some(right)) =>
                            //skipList.put(rightKey, rightValue) //right is not required here. It will eventually get added as more splits occur.
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            //                            println(s"Split mid   : ${midKey.readInt()} -> ${asString(midValue)}")
                            //                            println(s"Split right : ${rightKey.readInt()} -> ${asString(rightValue)}")

                            skipList.put(mid.fromKey, mid.copy(rangeValue = Value.Remove))
                            skipList.put(left.fromKey, left) //the previous entry was remove, so the rangeValue still remains removed.
                            (right, false)

                          case (left, Some(right), None) =>
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            //                            println(s"Split right : ${rightKey.readInt()} -> ${asString(rightValue)}")
                            if (left.fromKey equiv conflictingFromKey) { //if the split occurred at the root fromKey, set left to Remove
                              skipList.put(right.fromKey, right)
                              skipList.put(left.fromKey, left.copy(rangeValue = Value.Remove)) //the previous entry was remove, so the rangeValue still remains removed.
                              (right, true)
                            } else { //if the split occurred at top right, set right to Remove
                              val rightValueRemoved = right.copy(rangeValue = Value.Remove)
                              skipList.put(right.fromKey, rightValueRemoved)
                              skipList.put(left.fromKey, left) //the previous entry was remove, so the rangeValue still remains removed.
                              (rightValueRemoved, true)
                            }

                          case (left, None, None) =>
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            val leftRemoved = left.copy(rangeValue = Value.Remove)
                            skipList.put(left.fromKey, leftRemoved)
                            (left, true)
                        }

                      case conflictingRange: Memory.Range =>
                        //if fromValue is set then this is an actual key and should be updated.
                        conflictingRange.fromValue match {
                          case None => //if the ranges key-value does not have actual values set, remove them.
                            skipList.put(nextInsertRange.fromKey, nextInsertRange) //last range didn't contain any conflicting keys requiring splits. Add the last range.
                            if (conflictingRange.fromKey != nextInsertRange.fromKey) skipList.remove(conflictingRange.fromKey) //remove if it's not already replaced by above put key.
                            (nextInsertRange, true)

                          case Some(_) =>
                            val splitFromValue =
                              nextInsertRange.fromValue match {
                                case Some(value) if conflictingRange.fromKey equiv nextInsertRange.fromKey =>
                                  Some(value)
                                case _ =>
                                  conflictingRange.fromValue match {
                                    case removed @ Some(Value.Remove) => removed //if the key was remove, Update will not be applied, it will stay removed.
                                    case Some(_) => Some(insert.rangeValue) //if the key exists and was not remove, update will be applied to be the range's value.
                                    case None => None //else the key does not exists so there is no split value.
                                  }
                              }
                            split(conflictingRange.fromKey, splitFromValue, nextInsertRange) match {
                              case (left, right) =>
                                right foreach (right => skipList.put(right.fromKey, right))
                                skipList.put(left.fromKey, left)
                                (right getOrElse left, true)
                            }
                        }
                    }
                }
            } ==> {
            case (last, lastInserted) =>
              if (!lastInserted)
                skipList.put(last.fromKey, last)
          }
      }
    }
  }

  override def insert(insertKey: Slice[Byte],
                      insertValue: Memory,
                      skipList: ConcurrentSkipListMap[Slice[Byte], Memory])(implicit ordering: Ordering[Slice[Byte]]): Unit =
    insertValue match {
      //if insert value is fixed, check the floor entry
      case insertValue: Memory.Fixed =>
        insert(insertValue, skipList)

      //slice the skip list to keep on the range's key-values.
      //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
      //that fall within the inserted range before submitting fixed values to the range for further splits.
      case insertRange: Memory.Range =>
        insert(insertRange, skipList)
    }

  override def insert(entry: MapEntry[Slice[Byte], Memory],
                      skipList: ConcurrentSkipListMap[Slice[Byte], Memory])(implicit ordering: Ordering[Slice[Byte]]): Unit =
    entry match {
      case MapEntry.Put(key, value: Memory) =>
        insert(key, value, skipList)

      case MapEntry.Remove(_) =>
        entry applyTo skipList

      case _ =>
        entry.entries.foreach(insert(_, skipList))
    }
}