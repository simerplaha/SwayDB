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

import swaydb.core.data.Value
import swaydb.core.data.Value.Fixed
import swaydb.core.map.MapEntry.{Put, Remove}
import swaydb.core.map.{MapEntry, SkipListMerge}
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._

object LevelZeroSkipListMerge extends SkipListMerge[Slice[Byte], Value] {

  /**
    * Pre-requisite: splitKey should always be within range's fromKey and less than toKey.
    */
  private def split(splitKey: Slice[Byte],
                    splitFromValue: Option[Value.Fixed],
                    rangeFromKey: Slice[Byte],
                    range: Value.Range)(implicit ordering: Ordering[Slice[Byte]]): ((Slice[Byte], Value.Range), Option[(Slice[Byte], Value.Range)]) = {
    import ordering._
    if (splitKey equiv rangeFromKey) //if the splitKey == range's from key, update the fromKeyValue in the range.
      (
        (rangeFromKey, Value.Range(range.toKey, splitFromValue orElse range.fromValue, range.rangeValue)),
        None
      )
    else
      (
        (rangeFromKey, Value.Range(splitKey, range.fromValue, range.rangeValue)),
        Some((splitKey, Value.Range(range.toKey, splitFromValue, range.rangeValue)))
      )
  }

  //pre-requisite: split keys should always be greater than range's fromKey
  //3'rd range is returned None if the splitToKey > the range's toKey.
  private def split(splitFromKey: Slice[Byte],
                    splitToKey: Slice[Byte],
                    splitFromValue: Option[Value.Fixed],
                    rangeFromKey: Slice[Byte],
                    range: Value.Range)(implicit ordering: Ordering[Slice[Byte]]): ((Slice[Byte], Value.Range), Option[(Slice[Byte], Value.Range)], Option[(Slice[Byte], Value.Range)]) = {
    import ordering._
    split(splitFromKey, splitFromValue, rangeFromKey, range) match {
      case ((leftKey, leftValue), right @ Some((rightKey, rightValue))) =>
        if (splitToKey >= rightValue.toKey) {
          ((leftKey, leftValue), right, None)
        } else {
          val (mid, right) = split(splitToKey, None, rightKey, rightValue)
          ((leftKey, leftValue), Some(mid), right)
        }

      case (left @ (leftKey, leftValue), None) =>
        if (splitToKey >= leftValue.toKey) {
          (left, None, None)
        } else {
          val (left, mid) = split(splitToKey, None, leftKey, leftValue)
          (left, mid, None)
        }
    }
  }

  //split if the edge is a Range.
  private def adjustEdge(splitKey: Slice[Byte],
                         edgeFromKey: Slice[Byte],
                         edgeValue: Value,
                         skipList: ConcurrentSkipListMap[Slice[Byte], Value])(implicit ordering: Ordering[Slice[Byte]]) = {
    import ordering._
    edgeValue match {
      case edgeRange: Value.Range if splitKey > edgeFromKey && splitKey < edgeRange.toKey => //adjust only if within the range
        split(splitKey, None, edgeFromKey, edgeRange) match {
          case ((_, None)) =>
          //if only 1 split is returned then the
          //head fully overlaps the insert range's toKey.
          //no change required, the submission process will fix this ranges.
          case (((lowerRangeKey, lowerRangeValue), Some((upperRangeKey, upperRangeValue)))) =>
            //Split occurred, stash the lowerRange and keep the upperRange skipList.
            skipList.put(upperRangeKey, upperRangeValue) //put the upperRange, this will not alter the previous state of the skipList as only new entry is being added.
            skipList.put(lowerRangeKey, lowerRangeValue)
        }
      case _ =>
      //adjust not required
    }
  }

  /**
    * Inserts a [[Value.Fixed]] key-value into skipList.
    */
  def insert(insertKey: Slice[Byte],
             insertValue: Value.Fixed,
             skipList: ConcurrentSkipListMap[Slice[Byte], Value])(implicit ordering: Ordering[Slice[Byte]]): Unit = {
    import ordering._
    Option(skipList.floorEntry(insertKey)) match {
      case Some(floorEntry) =>
        val floorKey = floorEntry.getKey
        val floorValue = floorEntry.getValue
        floorValue match {
          //if floor entry for input fixed entry, simply put to replace the old entry if the keys are the same or it will as a new entry.
          case _: Fixed =>
            skipList.put(insertKey, insertValue)

          //if the floor entry is a range try to do a split. split function might not return splits if the insertKey
          //is greater than range's toKey since toKeys' of Ranges are exclusive.
          case floorRange: Value.Range if insertKey < floorRange.toKey => //if the fixed key is smaller then the range's toKey then do a split.
            split(insertKey, Some(insertValue), floorKey, floorRange) match {
              case (((leftKey, leftValue), right)) =>
                right foreach { case (key, value) => skipList.put(key, value) }
                skipList.put(leftKey, leftValue)
            }
          case _ =>
            skipList.put(insertKey, insertValue)
        }

      //if there is no floor, simply put.
      case None =>
        skipList.put(insertKey, insertValue)
    }
  }

  /**
    * Inserts the input [[Value.Range]] key-value into skipList and  always maintaining the previous state of
    * the skipList before applying the new state so that all read queries read the latest write.
    */
  def insert(insertKey: Slice[Byte],
             insertRange: Value.Range,
             skipList: ConcurrentSkipListMap[Slice[Byte], Value])(implicit ordering: Ordering[Slice[Byte]]): Unit = {
    import ordering._
    //get the start position of this range to fetch the range's start and end key-values for the skipList.
    val insertRangesFloorKey = Option(skipList.floorEntry(insertKey)) map {
      floorEntry =>
        floorEntry.getValue match {
          case _: Fixed if floorEntry.getKey >= insertKey =>
            floorEntry.getKey

          case range: Value.Range if insertKey < range.toKey =>
            floorEntry.getKey

          case _ =>
            insertKey
        }
    } getOrElse insertKey
    //fetch the key-values that fall within the range and submit all the key's that have fixed value set
    //to the new range and split the edge ranges that do not fall within the range.
    val conflictingRangeKeyValues = skipList.subMap(insertRangesFloorKey, true, insertRange.toKey, false)

    if (conflictingRangeKeyValues.isEmpty) {
      skipList.put(insertKey, insertRange)
    } else {
      //fix left edge.
      Option(conflictingRangeKeyValues.firstEntry()) foreach {
        left =>
          adjustEdge(insertKey, left.getKey, left.getValue, skipList)
      }

      //fix right edge.
      Option(conflictingRangeKeyValues.lastEntry()) foreach {
        right =>
          adjustEdge(insertRange.toKey, right.getKey, right.getValue, skipList)
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

      (insertRange.fromValue, insertRange.rangeValue) match {
        case (_, Value.Remove) =>
          skipList.put(insertKey, insertRange)
          skipList.subMap(insertKey, false, insertRange.toKey, false).clear()

        case (_, Value.Put(_)) =>
          skipList.subMap(insertKey, true, insertRange.toKey, false)
            .asScala
            .foldLeft((insertKey, insertRange)) {
              case result @ ((lastRangeKey, lastRangeValue), (conflictingKey, conflictingValue)) =>
                //                println(s"Current lastRange: ${lastRangeKey.readInt()} -> ${asString(lastRangeValue)}")
                //                println(s"Conflicting range: ${conflictingKey.readInt()} -> ${asString(conflictingValue)}")

                result match {
                  case ((nextInsertRangeFromKey, nextInsertRangeValue), (conflictingKey, conflictingValue)) =>

                    conflictingValue match {
                      case _: Fixed =>
                        val conflictingKeyValue =
                          nextInsertRangeValue.fromValue match {
                            case Some(value) if conflictingKey equiv nextInsertRangeFromKey =>
                              value
                            case _ =>
                              if (conflictingValue.isRemove)
                                Value.Remove
                              else
                                insertRange.rangeValue
                          }

                        val (left @ (leftKey, leftValue), right) = split(conflictingKey, Some(conflictingKeyValue), nextInsertRangeFromKey, nextInsertRangeValue)
                        right foreach { case (key, value) => skipList.put(key, value) }
                        skipList.put(leftKey, leftValue)
                        right getOrElse left

                      //match if conflicting range was remove. Updates cannot be applied on removed ranges, removed ranges are kept as is.
                      case Value.Range(conflictingToKey, conflictingFromValue, Value.Remove) =>
                        //if fromValue is set then this is an actual key and should be updated.
                        val splitFromValue =
                          nextInsertRangeValue.fromValue match {
                            case Some(value) if conflictingKey equiv nextInsertRangeFromKey =>
                              Some(value)
                            case _ =>
                              conflictingFromValue match {
                                case removed @ Some(Value.Remove) => removed //if the key was remove, Update will not be applied, it will stay removed.
                                case Some(_) => Some(insertRange.rangeValue) //if the key exists and was not remove, update will be applied to be the range's value.
                                case None => None //else the key does not exists so there is no split value.
                              }
                          }

                        split(conflictingKey, conflictingToKey, splitFromValue, nextInsertRangeFromKey, nextInsertRangeValue) match {
                          case ((leftKey, leftValue), Some((midKey, midValue)), Some((rightKey, rightValue))) =>
                            //skipList.put(rightKey, rightValue) //right is not required here. It will eventually get added as more splits occur.
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            //                            println(s"Split mid   : ${midKey.readInt()} -> ${asString(midValue)}")
                            //                            println(s"Split right : ${rightKey.readInt()} -> ${asString(rightValue)}")

                            skipList.put(midKey, midValue.copy(rangeValue = Value.Remove))
                            skipList.put(leftKey, leftValue) //the previous entry was remove, so the rangeValue still remains removed.
                            (rightKey, rightValue)

                          case ((leftKey, leftValue), Some((rightKey, rightValue)), None) =>
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            //                            println(s"Split right : ${rightKey.readInt()} -> ${asString(rightValue)}")
                            if (leftKey equiv conflictingKey) { //if the split occurred at the root fromKey, set left to Remove
                              skipList.put(rightKey, rightValue)
                              skipList.put(leftKey, leftValue.copy(rangeValue = Value.Remove)) //the previous entry was remove, so the rangeValue still remains removed.
                              (rightKey, rightValue)
                            } else { //if the split occurred at top right, set right to Remove
                              val rightValueRemoved = rightValue.copy(rangeValue = Value.Remove)
                              skipList.put(rightKey, rightValueRemoved)
                              skipList.put(leftKey, leftValue) //the previous entry was remove, so the rangeValue still remains removed.
                              (rightKey, rightValueRemoved)
                            }

                          case ((leftKey, leftValue), None, None) =>
                            //                            println(s"Split left  : ${leftKey.readInt()} -> ${asString(leftValue)}")
                            val leftRemoved = leftValue.copy(rangeValue = Value.Remove)
                            skipList.put(leftKey, leftRemoved)
                            (leftKey, leftRemoved)
                        }

                      case conflictingRange: Value.Range =>
                        //if fromValue is set then this is an actual key and should be updated.
                        conflictingRange.fromValue match {
                          case None => //if the ranges key-value does not have actual values set, remove them.
                            skipList.put(nextInsertRangeFromKey, nextInsertRangeValue) //last range didn't contain any conflicting keys requiring splits. Add the last range.
                            if (conflictingKey != nextInsertRangeFromKey) skipList.remove(conflictingKey) //remove if it's not already replaced by above put key.
                            (nextInsertRangeFromKey, nextInsertRangeValue)

                          case Some(_) =>
                            val splitFromValue =
                              nextInsertRangeValue.fromValue match {
                                case Some(value) if conflictingKey equiv nextInsertRangeFromKey =>
                                  Some(value)
                                case _ =>
                                  conflictingRange.fromValue match {
                                    case removed @ Some(Value.Remove) => removed //if the key was remove, Update will not be applied, it will stay removed.
                                    case Some(_) => Some(insertRange.rangeValue) //if the key exists and was not remove, update will be applied to be the range's value.
                                    case None => None //else the key does not exists so there is no split value.
                                  }
                              }
                            split(conflictingKey, splitFromValue, nextInsertRangeFromKey, nextInsertRangeValue) match {
                              case (left @ (leftKey, leftValue), right) =>
                                right foreach { case (key, value) => skipList.put(key, value) }
                                skipList.put(leftKey, leftValue)
                                right getOrElse left
                            }
                        }
                    }
                }
            } ==> {
            case (lastKey, lastValue) =>
              skipList.put(lastKey, lastValue)
          }
      }
    }
  }

  override def insert(insertKey: Slice[Byte],
                      insertValue: Value,
                      skipList: ConcurrentSkipListMap[Slice[Byte], Value])(implicit ordering: Ordering[Slice[Byte]]): Unit =
    insertValue match {
      //if insert value is fixed, check the floor entry
      case insertValue: Fixed =>
        insert(insertKey, insertValue, skipList)

      //slice the skip list to keep on the range's key-values.
      //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
      //that fall within the inserted range before submitting fixed values to the range for further splits.
      case insertRange: Value.Range =>
        insert(insertKey, insertRange, skipList)
    }

  override def insert(entry: MapEntry[Slice[Byte], Value],
                      skipList: ConcurrentSkipListMap[Slice[Byte], Value])(implicit ordering: Ordering[Slice[Byte]]): Unit =
    entry match {
      case Put(key, value: Value) =>
        insert(key, value, skipList)

      case Remove(_) =>
        entry applyTo skipList

      case _ =>
        entry.entries.foreach(insert(_, skipList))
    }
}