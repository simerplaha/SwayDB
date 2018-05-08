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

package swaydb.core.finders

import swaydb.core.data.KeyValue
import swaydb.core.segment.KeyValueMerger
import swaydb.data.slice.Slice

import scala.concurrent.duration._
import scala.util.{Success, Try}

object Higher {

  def apply(key: Slice[Byte],
            higherFromCurrentLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            ceiling: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]],
            higherInNextLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]])(implicit ordering: Ordering[Slice[Byte]]): Try[Option[KeyValue.ReadOnly.Put]] = {

    import ordering._

    /**
      * Given two key-values returns the highest. If both are expired then it fetches the next highest.
      * This function never invokes higherInNextLevel. 'next' should be pre-fetched and applied to the current Level's
      * highest Range key-value (if overlapping required) before invoking this function.
      *
      * Pre-requisite: current.key and next.key should be equal.
      */
    def returnLowest(current: KeyValue.ReadOnly.Fixed,
                     next: Option[KeyValue.ReadOnly.Put]): Try[Option[KeyValue.ReadOnly.Put]] =
      current match {
        case current: KeyValue.ReadOnly.Put =>
          next match {
            case Some(next) =>
              //if the Put from current Level is still active, compare it with the next Level's highest and return the returnLowest of both.
              if (current.hasTimeLeft()) {
                //    2
                //    2  or  5
                if (current.key <= next.key)
                  Success(Some(current))
                //    2
                //0
                else //else higher from next is smaller
                  Success(Some(next))
              } else { // current Level's put is expired. If the next Level's key is smaller, return it.
                //       2
                //0 or 1
                if (next.key < current.key)
                  Success(Some(next))
                //     2
                //     2 or 5
                // else if next Level's higher is equal to greater than this Level's higher.
                // equals should also be ignored because current Level's expired removed Put has overwritten next Level's Put.
                else
                  higher(current.key)
              }

            case None =>
              if (current.hasTimeLeft())
                Success(Some(current))
              else
                higher(current.key)
          }

        case current: KeyValue.ReadOnly.Remove =>
          next match {
            case Some(next) =>
              if (current.hasTimeLeft()) {
                //    2
                //    2
                if (next.key equiv current.key)
                  Success(current.deadline.map(next.updateDeadline) orElse Some(next))

                //    2
                //         5
                else if (next.key > current.key)
                  higher(current.key)

                //    2
                //0
                else
                  Success(Some(next))
              } else { //higher remove from current is expired.
                //    2
                //0
                //if the higher from next is smaller than current Remove. return it.
                if (next.key < current.key)
                  Success(Some(next))
                //     2
                //     2 or 5
                // else if next Level's higher is equal to greater than this Level's higher.
                // equals should also be ignored because current Level's expired removed Put has overwritten next Level's Put.
                else
                  higher(current.key)
              }

            case None =>
              higher(current.key)
          }

        case current: KeyValue.ReadOnly.Update =>
          next match {
            case Some(next) =>
              if (current.hasTimeLeft()) {
                //    2
                //    2
                if (next.key equiv current.key)
                  Try {
                    current.deadline map {
                      _ =>
                        current.toPut()
                    } orElse {
                      next.deadline.map(current.toPut) orElse Some(current.toPut())
                    }
                  }

                //    2
                //         5
                else if (next.key > current.key)
                  higher(current.key)

                //    2
                //0
                else
                  Success(Some(next))
              } else { //higher update from current is expired.
                //    2
                //0
                //if the higher from next is smaller than current Remove. return it.
                if (next.key < current.key)
                  Success(Some(next))
                //     2
                //     2 or 5
                // else if next Level's higher is equal to greater than this Level's higher.
                // equals should also be ignored because current Level's expired Update has deleted the Put from the lower Level.
                else
                  higher(current.key)
              }

            case None =>
              higher(current.key)
          }
      }

    //    println(s"${rootPath}: Higher for key: " + key.readInt())
    def higher(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
      higherFromCurrentLevel(key) flatMap {

        /** HIGHER FIXED FROM CURRENT LEVEL **/
        case Some(current: KeyValue.ReadOnly.Fixed) => //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
          higherInNextLevel(key) flatMap {
            next =>
              returnLowest(current, next)
          }

        /** HIGHER RANGE FROM CURRENT LEVEL **/
        //example
        //10->20  (input keys)
        //10 - 20 (higher range from current Level)
        case Some(current: KeyValue.ReadOnly.Range) if key >= current.fromKey =>
          current.fetchRangeValue flatMap {
            rangeValue =>
              if (rangeValue.hasTimeLeft()) //if the current range is active fetch the highest from next Level and return highest from both Levels.
                higherInNextLevel(key) flatMap {
                  case Some(next) =>
                    //10->20       (input keys)
                    //10   -    20 (higher range)
                    //  11 -> 19   (higher possible keys from next)
                    if (next.key < current.toKey) //if the higher in next Level falls within the range
                      KeyValueMerger.applyValue(rangeValue.toMemory(next.key), next, Duration.Zero) flatMap {
                        current =>
                          returnLowest(current, None) //return applied value with next key-value as the current value.
                      }
                    //10->20
                    //10 - 20
                    //     20 ----to----> ∞
                    else //else next Level's higher key doesn't fall in the Range, get ceiling of toKey
                      ceiling(current.toKey)

                  case None => //range is not applied if there is no underlying key in lower Levels, jump to the next highest key (toKey).
                    ceiling(current.toKey)
                }
              else //if the rangeValue is expired then the higher is ceiling of toKey
                ceiling(current.toKey)
          }

        //if the input key is smaller than this Level's higher Range's fromKey.
        //example:
        //0           (input key)
        //    10 - 20 (higher range)
        case Some(current: KeyValue.ReadOnly.Range) =>
          higherInNextLevel(key) flatMap {
            case someNext @ Some(next) =>
              //0
              //      10 - 20
              //  1
              if (next.key < current.fromKey)
                Success(someNext)

              //0
              //      10 - 20
              //      10
              else if (next.key equiv current.fromKey)
                current.fetchFromOrElseRangeValue flatMap {
                  fromOrElseRangeValue =>
                    KeyValueMerger.applyValue(fromOrElseRangeValue.toMemory(current.fromKey), next, Duration.Zero) flatMap {
                      current =>
                        returnLowest(current, None) //return applied value with next key-value as the current value.
                    }
                }
              //0
              //      10  -  20
              //        11-19
              else if (next.key < current.toKey) //if the higher in next Level falls within the range.
                current.fetchFromAndRangeValue flatMap {
                  case (Some(fromValue), _) =>
                    returnLowest(fromValue.toMemory(current.fromKey), None)

                  case (None, rangeValue) =>
                    KeyValueMerger.applyValue(rangeValue.toMemory(next.key), next, Duration.Zero) flatMap {
                      current =>
                        returnLowest(current, None) //return applied value as the current key-value.
                    }
                }

              //0
              //      10 - 20
              //           20 ----to----> ∞
              else //else if the higher in next Level does not fall within the range.
                current.fetchFromValue flatMap {
                  case Some(fromValue) =>
                    //next does not need to supplied here because it's already know that 'next' is larger than fromValue and there is not the highest.
                    //returnHigher will execute higher on fromKey if the the current does not result to a valid higher key-value.
                    returnLowest(fromValue.toMemory(current.fromKey), None)

                  case None =>
                    ceiling(current.toKey)
                }

            //no higher key-value in the next Level. Return fromValue orElse jump to toKey.
            case None =>
              current.fetchFromValue flatMap {
                fromValue =>
                  fromValue map {
                    fromValue =>
                      returnLowest(fromValue.toMemory(current.fromKey), None)
                  } getOrElse ceiling(current.toKey)
              }
          }

        case None =>
          higherInNextLevel(key)
      }

    higher(key)
  }
}