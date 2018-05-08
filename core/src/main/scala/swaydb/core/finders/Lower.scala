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

import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

object Lower {

  def apply(key: Slice[Byte],
            lowerFromCurrentLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            lowerFromNextLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]])(implicit ordering: Ordering[Slice[Byte]],
                                                                                   keyValueOrdering: Ordering[KeyValue.ReadOnly.Fixed]): Try[Option[KeyValue.ReadOnly.Put]] = {
    import ordering._

    /**
      * Given two key-values returns the lowest. If both are expired then it fetches the next lowest.
      * This function never invokes lowerInNextLevel. 'next' should be pre-fetched and applied to the current Level's
      * lowest Range key-value (if overlapping required) before invoking this function.
      *
      * Pre-requisite: current.key and next.key should be equal.
      */
    def returnHighest(current: KeyValue.ReadOnly.Fixed,
                      next: Option[KeyValue.ReadOnly.Put]): Try[Option[KeyValue.ReadOnly.Put]] =
      current match {
        case current: KeyValue.ReadOnly.Put =>
          next match {
            case Some(next) =>
              if (current.hasTimeLeft()) {
                //    2  or  5   (current)
                //    2          (next)
                if (current.key >= next.key)
                  Success(Some(current))
                //0          (current)
                //    2      (next)
                else //else next is highest
                  Success(Some(next))
              } else {
                //0 or 1      (current)
                //       2    (next)
                if (next.key > current.key)
                  Success(Some(next))
                //     2
                //     2
                else
                  lower(current.key)
              }

            case None =>
              if (current.hasTimeLeft())
                Success(Some(current))
              else
                lower(current.key)
          }

        case current: KeyValue.ReadOnly.Remove =>
          next match {
            case Some(next) =>
              if (current.hasTimeLeft()) {
                //    2
                //    2
                if (current.key equiv next.key)
                  Success(current.deadline.map(next.updateDeadline) orElse Some(next))

                //    2
                //0
                else if (current.key > next.key)
                  lower(current.key)

                //    2
                //         5
                else
                  Success(Some(next))
              } else { //lower remove from current is expired.
                //    2
                //        5
                if (next.key > current.key)
                  Success(Some(next))
                //     2
                //0 or 2
                else
                  lower(current.key)
              }

            case None =>
              lower(current.key)
          }

        case current: KeyValue.ReadOnly.Update =>
          next match {
            case Some(next) =>
              if (current.hasTimeLeft()) {
                //    2
                //    2
                if (current.key equiv next.key)
                  Try {
                    current.deadline map {
                      _ =>
                        current.toPut()
                    } orElse {
                      next.deadline.map(current.toPut) orElse Some(current.toPut())
                    }
                  }
                //    2
                //0
                else if (current.key > next.key)
                  lower(current.key)

                //    2
                //         5
                else
                  Success(Some(next))
              } else { //lower update from current is expired.
                //    2
                //         5
                if (next.key > current.key)
                  Success(Some(next))
                //     2
                //0 or 2
                else
                  lower(current.key)
              }

            case None =>
              lower(current.key)
          }
      }

    //    println(s"${rootPath}: Lower for key: " + key.readInt())
    def doLower(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
      lowerFromCurrentLevel(key) flatMap {
        /** LOWER FIXED FROM CURRENT LEVEL **/
        case Some(current: KeyValue.ReadOnly.Fixed) => //if the lower from the current Level is a Fixed key-value, fetch from next Level and return the lowest.
          lowerFromNextLevel(key) flatMap {
            next =>
              returnHighest(current, next)
          }

        /** LOWER RANGE FROM CURRENT LEVEL **/
        //example
        //     20 (input key)
        //10 - 20 (lower range from current Level)
        case Some(current: KeyValue.ReadOnly.Range) if key <= current.toKey =>
          current.fetchFromAndRangeValue flatMap {
            case (fromValue, rangeValue) =>
              if (rangeValue.hasTimeLeft()) //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                lowerFromNextLevel(key) flatMap {
                  case Some(next) =>
                    //  11 ->   20 (input keys)
                    //10   -    20 (lower range)
                    //  11 -> 19   (lower possible keys from next)
                    if (next.key > current.fromKey) //if the lower in next Level falls within the range
                      KeyValueMerger.applyValue(rangeValue.toMemory(next.key), next, Duration.Zero) flatMap {
                        current =>
                          returnHighest(current, None) //return applied value with next key-value as the current value.
                      }

                    //  11 ->   20 (input keys)
                    //10   -    20 (lower range)
                    //10
                    else if (next.key equiv current.fromKey)
                      KeyValueMerger.applyValue(fromValue.getOrElse(rangeValue).toMemory(next.key), next, Duration.Zero) flatMap {
                        current =>
                          returnHighest(current, None) //return applied value with next key-value as the current value.
                      }


                    //       11 ->   20 (input keys)
                    //     10   -    20 (lower range)
                    //0->9
                    else
                      fromValue map {
                        fromValue =>
                          returnHighest(fromValue.toMemory(current.fromKey), None)
                      } getOrElse lower(current.fromKey)

                  case None =>
                    fromValue map {
                      fromValue =>
                        returnHighest(fromValue.toMemory(current.fromKey), None)
                    } getOrElse lower(current.fromKey)
                }
              else
                fromValue map {
                  fromValue =>
                    if (fromValue.hasTimeLeft())
                      lowerFromNextLevel(key) flatMap {
                        case Some(next) =>
                          //check for equality from lower Level with fromValue only.
                          if (current.key equiv next.key)
                            KeyValueMerger.applyValue(fromValue.toMemory(next.key), next, Duration.Zero) flatMap {
                              current =>
                                returnHighest(current, None) //return applied value with next key-value as the current value.
                            }
                          else
                            returnHighest(fromValue.toMemory(current.fromKey), None)

                        case None =>
                          returnHighest(fromValue.toMemory(current.fromKey), None)
                      }
                    else
                      lower(current.fromKey)
                } getOrElse lower(current.fromKey)
          }

        //example:
        //             22 (input key)
        //    10 - 20     (lower range)
        case Some(current: KeyValue.ReadOnly.Range) =>
          lowerFromNextLevel(key) flatMap {
            case someNext @ Some(next) =>

              //                     22 (input key)
              //      10 - 20
              //           20 or 21
              if (next.key >= current.toKey)
                Success(someNext)

              //                   22 (input key)
              //      10   -   20
              //         11->19
              else if (next.key > current.fromKey)
                current.fetchRangeValue flatMap {
                  rangeValue =>
                    KeyValueMerger.applyValue(rangeValue.toMemory(next.key), next, Duration.Zero) flatMap {
                      current =>
                        returnHighest(current, None) //return applied value with next key-value as the current value.
                    }
                }
              //                22 (input key)
              //      10  -  20
              //      10
              else if (next.key equiv current.fromKey) //if the lower in next Level falls within the range.
                current.fetchFromOrElseRangeValue flatMap {
                  fromOrElseRangeValue =>
                    KeyValueMerger.applyValue(fromOrElseRangeValue.toMemory(current.fromKey), next, Duration.Zero) flatMap {
                      current =>
                        returnHighest(current, None) //return applied value with next key-value as the current value.
                    }
                }
              //               22 (input key)
              //       10 - 20
              //0 to 9
              else
                current.fetchFromValue flatMap {
                  case Some(fromValue) =>
                    //next does not need to supplied here because it's already know that 'next' is larger than fromValue and there is not the lowest.
                    //returnLower will execute lower on fromKey if the the current does not result to a valid lower key-value.
                    returnHighest(fromValue.toMemory(current.fromKey), None)

                  case None =>
                    lower(current.fromKey)
                }

            //no lower key-value in the next Level. Return fromValue orElse jump to toKey.
            case None =>
              current.fetchFromValue flatMap {
                fromValue =>
                  fromValue map {
                    fromValue =>
                      returnHighest(fromValue.toMemory(current.fromKey), None)
                  } getOrElse lower(current.fromKey)
              }
          }

        case None =>
          lowerFromNextLevel(key)
      }

    def lower(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
      doLower(key)

    lower(key)
  }
}