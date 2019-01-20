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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.finders

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.FixedMerger
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[core] object Higher {

  //indicates that next Level was read but returned empty.
  //this marker is used to not read lower Level again.
  private val someNone = Some(None)

  /**
    * TO-DO - Use trampolining instead to reduce repeated boilerplate code.
    */
  def apply(key: Slice[Byte],
            higherFromCurrentLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.SegmentResponse]],
            get: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]],
            higherInNextLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                                  functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] = {

    import keyOrder._

    //stash next if it's key is larger than current's key so that the same next key can be used
    //in next iteration instead of re-reading next Level again.
    def stashNext(nextHigher: Option[Option[KeyValue.ReadOnly.Put]],
                  next: Option[KeyValue.ReadOnly.Put],
                  currentKey: Slice[Byte]): Option[Option[KeyValue.ReadOnly.Put]] =
      if (nextHigher.contains(None)) //if nextHigher was already set to empty, it stays empty so that next Level does not get read.
        nextHigher
      else if (next.isEmpty) //if next is empty this means next level was read and it returned none so set it to Higher.someNone indicating it's empty.
        Higher.someNone
      else
        next flatMap { //else next is defined so check if the next is still larger than current and keep stashing else set to None so next Level is re-read again.
          next =>
            //stash if next Level's key is still larger than current Levels. Don't need to check for time here because it will eventually get check during merge.
            if (next.key > currentKey)
              Some(Some(next))
            else
              None
        }

    //stash next if it's key is larger than current's key so that the same next key can be used
    //in next iteration instead of re-reading next Level again.
    def keepStashIfLargerKey(nextHigher: Option[Option[KeyValue.ReadOnly.Put]],
                             currentKey: Slice[Byte]): Option[Option[KeyValue.ReadOnly.Put]] =
      if (nextHigher.contains(None))
        nextHigher
      else
        nextHigher flatMap {
          next =>
            next map {
              next =>
                if (next.key > currentKey)
                  Some(next)
                else
                  None
            }
        }

    /**
      * This code is crucial to forward iteration performance. It should avoid any unnecessary seeks.
      *
      * @param currentRange      if set the current range is still the active highest in current Level and is
      *                          being merged into next Level's keys to find the next highest.
      * @param stashedNextHigher None(None) if higher from next level is not known and can be read.
      *                     Higher.someNone if there are no higher keys in next Level.
      *                          Some(Some) if higher from next Level is known but is too far off and does not overlap current. Stashed to process in next iteration.
      */
    @tailrec
    def higher(key: Slice[Byte],
               currentRange: Option[KeyValue.ReadOnly.Range],
               stashedNextHigher: Option[Option[KeyValue.ReadOnly.Put]]): Try[Option[KeyValue.ReadOnly.Put]] =
      currentRange.map(_ => Success(currentRange)) getOrElse higherFromCurrentLevel(key) match {
        case Success(current) =>
          current match {
            case Some(current) =>
              current match {
                /** HIGHER FIXED FROM CURRENT LEVEL **/
                case current: KeyValue.ReadOnly.Fixed =>
                  //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  stashedNextHigher.map(Success(_)) getOrElse higherInNextLevel(key) match {
                    case Success(next) =>
                      Min(current, next) match {
                        case found @ Success(Some(put)) if put.hasTimeLeft() =>
                          found

                        case Success(_) =>
                          higher(current.key, None, stashNext(stashedNextHigher, next, current.key))

                        case Failure(exception) =>
                          Failure(exception)
                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }

                /** HIGHER RANGE FROM CURRENT LEVEL **/
                //example
                //10->19  (input keys)
                //10 - 20 (higher range from current Level)
                case currentRange: KeyValue.ReadOnly.Range if key >= currentRange.fromKey =>
                  currentRange.fetchRangeValue match {
                    case Success(rangeValue) =>
                      //if the current range is active fetch the highest from next Level and return highest from both Levels.
                      if (Value.hasTimeLeft(rangeValue))
                      //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                        stashedNextHigher.map(Success(_)) getOrElse higherInNextLevel(key) match {
                          case Success(someNext @ Some(next)) =>
                            //10->20       (input keys)
                            //10   -    20 (higher range)
                            //  11 -> 19   (higher possible keys from next)
                            if (next.key < currentRange.toKey) //if the higher in next Level falls within the range
                              FixedMerger(rangeValue.toMemory(next.key), next) match {
                                case Success(current) =>
                                  //return applied value with next key-value as the current value.
                                  Min(current, None) match {
                                    case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                      found

                                    case Success(_) =>
                                      higher(next.key, Some(currentRange), None) //current.key or next.key (either one! they both are the same here)

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                                case Failure(exception) =>
                                  Failure(exception)
                              }


                            //10->19
                            //10 - 20
                            //     20 ----to----> ∞
                            else //else next Level's higher key doesn't fall in the Range, get ceiling of toKey
                              get(currentRange.toKey) match {
                                case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                  Success(Some(ceiling))

                                case Success(_) =>
                                  higher(currentRange.toKey, None, stashNext(stashedNextHigher, someNext, currentRange.toKey))

                                case failed @ Failure(_) =>
                                  failed
                              }

                          case Success(None) => //if there is no underlying key in lower Levels, jump to the next highest key (toKey).
                            get(currentRange.toKey) match {
                              case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                Success(Some(ceiling))

                              case Success(_) =>
                                higher(currentRange.toKey, None, Higher.someNone)

                              case failed @ Failure(_) =>
                                failed
                            }

                          case Failure(exception) =>
                            Failure(exception)
                        }

                      else //if the rangeValue is expired then the higher is ceiling of toKey
                        get(currentRange.toKey) match {
                          case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                            Success(Some(ceiling))

                          case Success(_) =>
                            higher(currentRange.toKey, None, keepStashIfLargerKey(stashedNextHigher, currentRange.toKey)) //don't know! as it's not fetched.

                          case failed @ Failure(_) =>
                            failed
                        }

                    case Failure(exception) =>
                      Failure(exception)
                  }

                //if the input key is smaller than this Level's higher Range's fromKey.
                //example:
                //0           (input key)
                //    10 - 20 (higher range)
                case currentRange: KeyValue.ReadOnly.Range =>
                  //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  stashedNextHigher.map(Success(_)) getOrElse higherInNextLevel(key) match {
                    case Success(next) =>
                      next match {
                        case someNext @ Some(next) =>
                          //0
                          //      10 - 20
                          //  1
                          if (next.key < currentRange.fromKey)
                            Success(someNext)

                          //0
                          //      10 - 20
                          //      10
                          else if (next.key equiv currentRange.fromKey)
                            currentRange.fetchFromOrElseRangeValue match {
                              case Success(fromOrElseRangeValue) =>
                                FixedMerger(fromOrElseRangeValue.toMemory(currentRange.fromKey), next) match {
                                  case Success(mergedCurrent) =>
                                    Min(mergedCurrent, None) match { //return applied value with next key-value as the current value.
                                      case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                        found

                                      case Success(_) =>
                                        higher(next.key, Some(currentRange), None) //current.key or next.key (either one! they both are the same here)

                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                  case Failure(exception) =>
                                    Failure(exception)
                                }

                              case Failure(exception) =>
                                Failure(exception)
                            }
                          //0
                          //      10  -  20
                          //        11-19
                          else if (next.key < currentRange.toKey) //if the higher in next Level falls within the range.
                            currentRange.fetchFromAndRangeValue match {
                              case Success((Some(fromValue), rangeValue)) => //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                                Min(fromValue.toMemory(currentRange.fromKey), None) match {
                                  case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                    found

                                  case Success(_) =>
                                    FixedMerger(rangeValue.toMemory(next.key), next) match {
                                      case Success(currentValue) =>
                                        Min(currentValue, None) match { //return applied value with next key-value as the current value.
                                          case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                            found

                                          case Success(_) =>
                                            higher(currentRange.fromKey, Some(currentRange), None)

                                          case Failure(exception) =>
                                            Failure(exception)
                                        }

                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                  case Failure(exception) =>
                                    Failure(exception)
                                }

                              case Success((None, rangeValue)) => //if there is no from key
                                FixedMerger(rangeValue.toMemory(next.key), next) match {
                                  case Success(current) =>
                                    Min(current, None) match { //return applied value with next key-value as the current value.
                                      case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                        found

                                      case Success(_) =>
                                        higher(next.key, Some(currentRange), None)

                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                  case Failure(exception) =>
                                    Failure(exception)
                                }

                              case Failure(exception) =>
                                Failure(exception)
                            }

                          //0
                          //      10 - 20
                          //           20 ----to----> ∞
                          else //else if the higher in next Level does not fall within the range.
                            currentRange.fetchFromValue match {
                              case Success(maybeFromValue) =>
                                maybeFromValue match {
                                  case Some(fromValue) =>
                                    //next does not need to supplied here because it's already know that 'next' is larger than fromValue and there is not the highest.
                                    //Min will execute higher on fromKey if the the current does not result to a valid higher key-value.
                                    Min(fromValue.toMemory(currentRange.fromKey), None) match { //return applied value with next key-value as the current value.
                                      case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                        found

                                      case Success(_) =>
                                        get(currentRange.toKey) match {
                                          case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                            Success(Some(ceiling))

                                          case Success(_) =>
                                            higher(currentRange.toKey, None, stashNext(stashedNextHigher, someNext, currentRange.toKey))

                                          case failed @ Failure(_) =>
                                            failed
                                        }

                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                  case None =>
                                    get(currentRange.toKey) match {
                                      case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                        Success(Some(ceiling))

                                      case Success(_) =>
                                        higher(currentRange.toKey, None, stashNext(stashedNextHigher, someNext, currentRange.toKey))

                                      case failed @ Failure(_) =>
                                        failed
                                    }
                                }

                              case Failure(exception) =>
                                Failure(exception)
                            }

                        //no higher key-value in the next Level. Return fromValue orElse jump to toKey.
                        case None =>
                          currentRange.fetchFromValue match {
                            case Success(maybeFromValue) =>
                              maybeFromValue match {
                                case Some(fromValue) =>
                                  Min(fromValue.toMemory(currentRange.fromKey), None) match {
                                    case found @ Success(Some(put)) if put.hasTimeLeft() =>
                                      found

                                    case Success(_) =>
                                      get(currentRange.toKey) match {
                                        case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                          Success(Some(ceiling))

                                        case Success(_) =>
                                          higher(currentRange.toKey, None, Higher.someNone)

                                        case failed @ Failure(_) =>
                                          failed
                                      }

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                                case None =>
                                  get(currentRange.toKey) match {
                                    case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                                      Success(Some(ceiling))

                                    case Success(_) =>
                                      higher(currentRange.toKey, None, Higher.someNone)

                                    case failed @ Failure(_) =>
                                      failed
                                  }
                              }
                            case Failure(exception) =>
                              Failure(exception)
                          }
                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }
              }

            case None =>
              stashedNextHigher.map(Success(_)) getOrElse higherInNextLevel(key)

          }
        case Failure(exception) =>
          Failure(exception)
      }

    higher(key, None, None)
  }
}
