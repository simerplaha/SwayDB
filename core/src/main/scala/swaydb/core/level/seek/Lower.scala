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

package swaydb.core.level.seek

import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge._
import swaydb.data.io.Core
import swaydb.data.io.Core.Error.Private.ErrorHandler
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[core] object Lower {

  /**
    * Check and returns the FromValue if it's a valid lower key-value for the input key.
    */
  def lowerFromValue(key: Slice[Byte],
                     fromKey: Slice[Byte],
                     fromValue: Option[Value.FromValue])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Memory.Put] = {
    import keyOrder._
    fromValue flatMap {
      fromValue =>
        if (fromKey < key)
          fromValue.toMemory(fromKey) match {
            case put: Memory.Put if put.hasTimeLeft() =>
              Some(put)

            case _ =>
              None
          }
        else
          None
    }
  }

  def seek(key: Slice[Byte],
           currentSeek: Seek.Current,
           nextSeek: Seek.Next,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentWalker: CurrentWalker,
           nextWalker: NextWalker,
           functionStore: FunctionStore): IO.Defer[Core.Error.Private, Option[KeyValue.ReadOnly.Put]] =
    Lower(key, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  def seeker(key: Slice[Byte],
             currentSeek: Seek.Current,
             nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  currentWalker: CurrentWalker,
                                  nextWalker: NextWalker,
                                  functionStore: FunctionStore): IO.Defer[Core.Error.Private, Option[KeyValue.ReadOnly.Put]] =
    Lower(key, currentSeek, nextSeek)

  /**
    * May be use trampolining instead and split the matches into their own functions to reduce
    * repeated boilerplate code & if does not effect read performance or adds to GC workload.
    *
    * This and [[Higher]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
    * could help share this code and removing duplicates but only if there is no performance penalty.
    */
  @tailrec
  def apply(key: Slice[Byte],
            currentSeek: Seek.Current,
            nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 currentWalker: CurrentWalker,
                                 nextWalker: NextWalker,
                                 functionStore: FunctionStore): IO.Defer[Core.Error.Private, Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    (currentSeek, nextSeek) match {
      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Next  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Read, Seek.Read) =>
        currentWalker.lower(key) match {
          case IO.Success(Some(lower)) =>
            Lower(key, Seek.Current.Stash(lower), nextSeek)

          case IO.Success(None) =>
            Lower(key, Seek.Current.Stop, nextSeek)

          case failure @ IO.Failure(_) =>
            failure
              .recoverToDeferred(
                Lower.seeker(key, currentSeek, nextSeek)
              )
        }

      case (currentStash @ Seek.Current.Stash(current), Seek.Read) =>
        //decide if it's necessary to read the next Level or not.
        current match {
          //   19   (input key - exclusive)
          //10 - 20 (lower range from current Level)
          case currentRange: ReadOnly.Range if key < currentRange.toKey =>
            currentRange.fetchRangeValue match {
              case IO.Success(rangeValue) =>
                //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                if (Value.hasTimeLeft(rangeValue)) {
                  val nextStateID = nextWalker.stateID
                  nextWalker.lower(key) match {
                    case IO.Success(Some(next)) =>
                      Lower(key, currentStash, Seek.Next.Stash(next, nextStateID))

                    case IO.Success(None) =>
                      Lower(key, currentStash, Seek.Next.Stop(nextStateID))

                    case later @ IO.Deferred(_, _) =>
                      later flatMap {
                        case Some(next) =>
                          Lower.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                        case None =>
                          Lower.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                      }

                    case failure @ IO.Failure(_) =>
                      failure
                  }
                }

                //if the rangeValue is expired then check if fromValue is valid put else fetch from lower and merge.
                else
                  currentRange.fetchFromValue match {
                    case IO.Success(maybeFromValue) =>
                      //check if from value is a put before reading the next Level.
                      lowerFromValue(key, currentRange.fromKey, maybeFromValue) match {
                        case some @ Some(_) => //yes it is!
                          IO.Success(some)

                        //if not, then fetch lower of key in next Level.
                        case None => {
                          val nextStateID = nextWalker.stateID
                          nextWalker.lower(key) match {
                            case IO.Success(Some(next)) =>
                              Lower(key, currentStash, Seek.Next.Stash(next, nextStateID))

                            case IO.Success(None) =>
                              Lower(key, currentStash, Seek.Next.Stop(nextStateID))

                            case later @ IO.Deferred(_, _) =>
                              later flatMap {
                                case Some(next) =>
                                  Lower.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                                case None =>
                                  Lower.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                              }

                            case failure @ IO.Failure(_) =>
                              failure
                          }
                        }
                      }

                    case failure @ IO.Failure(_) =>
                      failure
                        .recoverToDeferred(
                          Lower.seeker(key, currentSeek, nextSeek)
                        )
                  }

              case failure @ IO.Failure(_) =>
                failure
                  .recoverToDeferred(
                    Lower.seeker(key, currentSeek, nextSeek)
                  )
            }

          //     20 (input key - inclusive)
          //10 - 20 (lower range from current Level)
          case currentRange: ReadOnly.Range if key equiv currentRange.toKey =>
            val nextStateID = nextWalker.stateID
            //lower level could also contain a toKey but toKey is exclusive to merge is not required but lower level is read is required.
            nextWalker.lower(key) match {
              case IO.Success(Some(nextFromKey)) =>
                Lower(key, currentStash, Seek.Next.Stash(nextFromKey, nextStateID))

              case IO.Success(None) =>
                Lower(key, currentStash, Seek.Next.Stop(nextStateID))

              case later @ IO.Deferred(_, _) =>
                later flatMap {
                  case Some(nextFromKey) =>
                    Lower.seeker(key, currentStash, Seek.Next.Stash(nextFromKey, nextStateID))

                  case None =>
                    Lower.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                }

              case failure @ IO.Failure(_) =>
                failure
            }

          //             22 (input key)
          //    10 - 20     (lower range)
          case _: KeyValue.ReadOnly.Range =>
            val nextStateID = nextWalker.stateID
            nextWalker.lower(key) match {
              case IO.Success(Some(next)) =>
                Lower(key, currentStash, Seek.Next.Stash(next, nextStateID))

              case IO.Success(None) =>
                Lower(key, currentStash, Seek.Next.Stop(nextStateID))

              case later @ IO.Deferred(_, _) =>
                later flatMap {
                  case Some(next) =>
                    Lower.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                  case None =>
                    Lower.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                }

              case failure @ IO.Failure(_) =>
                failure
            }

          case _: ReadOnly.Fixed =>
            val nextStateID = nextWalker.stateID
            nextWalker.lower(key) match {
              case IO.Success(Some(next)) =>
                Lower(key, currentStash, Seek.Next.Stash(next, nextStateID))

              case IO.Success(None) =>
                Lower(key, currentStash, Seek.Next.Stop(nextStateID))

              case later @ IO.Deferred(_, _) =>
                later flatMap {
                  case Some(next) =>
                    Lower.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                  case None =>
                    Lower.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                }

              case failure @ IO.Failure(_) =>
                failure
            }
        }

      case (Seek.Current.Stop, Seek.Read) =>
        val nextStateID = nextWalker.stateID
        nextWalker.lower(key) match {
          case IO.Success(Some(next)) =>
            Lower(key, currentSeek, Seek.Next.Stash(next, nextStateID))

          case IO.Success(None) =>
            Lower(key, currentSeek, Seek.Next.Stop(nextStateID))

          case later @ IO.Deferred(_, _) =>
            later flatMap {
              case Some(next) =>
                Lower.seeker(key, currentSeek, Seek.Next.Stash(next, nextStateID))

              case None =>
                Lower.seeker(key, currentSeek, Seek.Next.Stop(nextStateID))
            }

          case failure @ IO.Failure(_) =>
            failure
        }

      /** *********************************************************
        * ******************                    *******************
        * ******************  Current on Stash  *******************
        * ******************                    *******************
        * *********************************************************/

      case (Seek.Current.Stop, Seek.Next.Stash(next, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else if (next.hasTimeLeft())
          IO.Success(Some(next))
        else
          Lower(next.key, currentSeek, Seek.Read)

      case (Seek.Read, Seek.Next.Stash(_, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else
          currentWalker.lower(key) match {
            case IO.Success(Some(current)) =>
              Lower(key, Seek.Current.Stash(current), nextSeek)

            case IO.Success(None) =>
              Lower(key, Seek.Current.Stop, nextSeek)

            case failure @ IO.Failure(_) =>
              failure
                .recoverToDeferred(
                  Lower.seeker(key, currentSeek, nextSeek)
                )
          }

      case (currentStash @ Seek.Current.Stash(current), nextStash @ Seek.Next.Stash(next, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else
          (current, next) match {

            /** **********************************************
              * ******************         *******************
              * ******************  Fixed  *******************
              * ******************         *******************
              * **********************************************/

            case (current: KeyValue.ReadOnly.Fixed, next: KeyValue.ReadOnly.Fixed) =>
              //    2
              //    2
              if (next.key equiv current.key)
                FixedMerger(current, next) match {
                  case IO.Success(merged) =>
                    merged match {
                      case put: ReadOnly.Put if put.hasTimeLeft() =>
                        IO.Success(Some(put))

                      case _ =>
                        //if it doesn't result in an unexpired put move forward.
                        Lower(current.key, Seek.Read, Seek.Read)
                    }
                  case failure @ IO.Failure(_) =>
                    failure
                      .recoverToDeferred(
                        Lower.seeker(key, currentSeek, nextSeek)
                      )
                }
              //      3  or  5 (current)
              //    1          (next)
              else if (current.key > next.key)
                current match {
                  case put: ReadOnly.Put if put.hasTimeLeft() =>
                    IO.Success(Some(put))

                  //if it doesn't result in an unexpired put move forward.
                  case _ =>
                    Lower(current.key, Seek.Read, nextStash)
                }
              //0
              //    2
              else //else lower from next is the lowest.
                IO.Success(Some(next))

            /** *********************************************
              * *********************************************
              * ******************       ********************
              * ****************** RANGE ********************
              * ******************       ********************
              * *********************************************
              * *********************************************/
            case (current: KeyValue.ReadOnly.Range, next: KeyValue.ReadOnly.Fixed) =>
              //             22 (input)
              //10 - 20         (current)
              //     20 - 21    (next)
              if (next.key >= current.toKey)
                IO.Success(Some(next))

              //  11 ->   20 (input keys)
              //10   -    20 (lower range)
              //  11 -> 19   (lower possible keys from next)
              else if (next.key > current.fromKey)
                current.fetchRangeValue match {
                  case IO.Success(rangeValue) =>
                    FixedMerger(rangeValue.toMemory(next.key), next) match {
                      case IO.Success(mergedCurrent) =>
                        mergedCurrent match {
                          case put: ReadOnly.Put if put.hasTimeLeft() =>
                            IO.Success(Some(put))

                          case _ =>
                            //do need to check if range is expired because if it was then
                            //next would not have been read from next level in the first place.
                            Lower(next.key, currentStash, Seek.Read)
                        }

                      case failure @ IO.Failure(_) =>
                        failure
                          .recoverToDeferred(
                            Lower.seeker(key, currentSeek, nextSeek)
                          )
                    }

                  case failure @ IO.Failure(_) =>
                    failure
                      .recoverToDeferred(
                        Lower.seeker(key, currentSeek, nextSeek)
                      )
                }

              //  11 ->   20 (input keys)
              //10   -    20 (lower range)
              //10
              else if (next.key equiv current.fromKey) //if the lower in next Level falls within the range.
                current.fetchFromOrElseRangeValue match {
                  //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                  case IO.Success(rangeValue) =>
                    lowerFromValue(key, current.fromKey, Some(rangeValue)) match {
                      case lowerPut @ Some(_) =>
                        IO.Success(lowerPut)

                      //fromValue is not put, check if merging is required else return next.
                      case None =>
                        FixedMerger(rangeValue.toMemory(next.key), next) match {
                          case IO.Success(mergedValue) =>
                            mergedValue match { //return applied value with next key-value as the current value.
                              case put: Memory.Put if put.hasTimeLeft() =>
                                IO.Success(Some(put))

                              case _ =>
                                Lower(next.key, Seek.Read, Seek.Read)
                            }

                          case failure @ IO.Failure(_) =>
                            failure
                              .recoverToDeferred(
                                Lower.seeker(key, currentSeek, nextSeek)
                              )
                        }
                    }

                  case failure @ IO.Failure(_) =>
                    failure
                      .recoverToDeferred(
                        Lower.seeker(key, currentSeek, nextSeek)
                      )
                }

              //       11 ->   20 (input keys)
              //     10   -    20 (lower range)
              //0->9
              else //else if the lower in next Level does not fall within the range.
                current.fetchFromValue match {
                  //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                  case IO.Success(fromValue) =>
                    lowerFromValue(key, current.fromKey, fromValue) match {
                      case somePut @ Some(_) =>
                        IO.Success(somePut)

                      case None =>
                        Lower(current.fromKey, Seek.Read, nextStash)
                    }

                  case failure @ IO.Failure(_) =>
                    failure
                      .recoverToDeferred(
                        Lower.seeker(key, currentSeek, nextSeek)
                      )
                }
          }

      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Stop  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Read, Seek.Next.Stop(nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else
          currentWalker.lower(key) match {
            case IO.Success(Some(current)) =>
              Lower(key, Seek.Current.Stash(current), nextSeek)

            case IO.Success(None) =>
              Lower(key, Seek.Current.Stop, nextSeek)

            case failure @ IO.Failure(_) =>
              failure
                .recoverToDeferred(
                  Lower.seeker(key, currentSeek, nextSeek)
                )
          }

      case (Seek.Current.Stash(current), Seek.Next.Stop(nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else
          current match {
            case current: KeyValue.ReadOnly.Put =>
              if (current.hasTimeLeft())
                IO.Success(Some(current))
              else
                Lower(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Remove =>
              Lower(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Update =>
              Lower(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Function =>
              Lower(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.PendingApply =>
              Lower(current.key, Seek.Read, nextSeek)

            case current: KeyValue.ReadOnly.Range =>
              current.fetchFromValue match {
                case IO.Success(fromValue) =>
                  lowerFromValue(key, current.fromKey, fromValue) match {
                    case somePut @ Some(_) =>
                      IO.Success(somePut)

                    case None =>
                      Lower(current.fromKey, Seek.Read, nextSeek)
                  }

                case failure @ IO.Failure(_) =>
                  failure
                    .recoverToDeferred(
                      Lower.seeker(key, currentSeek, nextSeek)
                    )
              }
          }

      case (Seek.Current.Stop, Seek.Next.Stop(nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Lower(key, currentSeek, Seek.Read)
        else
          IO.none
    }
  }
}
