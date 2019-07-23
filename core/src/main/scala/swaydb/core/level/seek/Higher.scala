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
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[core] object Higher {

  /**
    * Check and returns the FromValue if it's a valid higher key-value for the input key
    */
  def higherFromValue(key: Slice[Byte],
                      fromKey: Slice[Byte],
                      fromValue: Option[Value.FromValue])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Memory.Put] = {
    import keyOrder._
    fromValue flatMap {
      fromValue =>
        if (fromKey > key)
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
           functionStore: FunctionStore): IO.Defer[Option[KeyValue.ReadOnly.Put]] =
    Higher(key, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  /**
    * Just another function that is not tailrec for delayed Higher fetches.
    */
  private def seeker(key: Slice[Byte],
                     currentSeek: Seek.Current,
                     nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          timeOrder: TimeOrder[Slice[Byte]],
                                          currentWalker: CurrentWalker,
                                          nextWalker: NextWalker,
                                          functionStore: FunctionStore): IO.Defer[Option[KeyValue.ReadOnly.Put]] =
    Higher(key, currentSeek, nextSeek)

  /**
    * May be use trampolining instead and split the matches into their own functions to reduce
    * repeated boilerplate code & if does not effect read performance or adds to GC workload.
    *
    * This and [[Lower]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
    * could help share this code and removing duplicates but only if there is no performance penalty.
    */
  @tailrec
  def apply(key: Slice[Byte],
            currentSeek: Seek.Current,
            nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 currentWalker: CurrentWalker,
                                 nextWalker: NextWalker,
                                 functionStore: FunctionStore): IO.Defer[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    //    println(s"Current walker: ${currentWalker.levelNumber} - ${key.readInt()}")

    (currentSeek, nextSeek) match {
      /** *********************************************************
        * ******************                    *******************
        * ******************  Current on Fetch  *******************
        * ******************                    *******************
        * ********************************************************/

      case (Seek.Read, Seek.Read) =>
        currentWalker.higher(key) match {
          case IO.Success(Some(higher)) =>
            Higher(key, Seek.Current.Stash(higher), nextSeek)

          case IO.Success(None) =>
            Higher(key, Seek.Current.Stop, nextSeek)

          case failure: IO.Failure[_] =>
            failure
              .recoverToAsync(
                Higher.seeker(key, currentSeek, nextSeek)
              )
        }

      case (currentStash @ Seek.Current.Stash(current), Seek.Read) =>
        //decide if it's necessary to read the next Level or not.
        current match {
          //10->19  (input keys)
          //10 - 20 (higher range from current Level)
          case currentRange: ReadOnly.Range if key >= currentRange.fromKey =>
            currentRange.fetchRangeValue match {
              case IO.Success(rangeValue) =>
                //if the current range is active fetch the highest from next Level and return highest from both Levels.
                if (Value.hasTimeLeft(rangeValue)) {
                  //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  val nextStateID = nextWalker.stateID
                  nextWalker.higher(key) match {
                    case IO.Success(Some(next)) =>
                      Higher(key, currentStash, Seek.Next.Stash(next, nextStateID))

                    case IO.Success(None) =>
                      Higher(key, currentStash, Seek.Next.Stop(nextStateID))

                    case later @ IO.Deferred(_, _) =>
                      later flatMap {
                        case Some(next) =>
                          Higher.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                        case None =>
                          Higher.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                      }

                    case failure: IO.Failure[_] =>
                      failure
                  }
                }

                else //if the rangeValue is expired then the higher is ceiling of toKey
                  currentWalker.get(currentRange.toKey) match {
                    case IO.Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                      IO.Success(Some(ceiling))

                    case IO.Success(_) =>
                      Higher(currentRange.toKey, Seek.Read, nextSeek)

                    case later @ IO.Deferred(_, _) =>
                      later flatMap {
                        case Some(ceiling) if ceiling.hasTimeLeft() =>
                          IO.Success(Some(ceiling))

                        case Some(_) | None =>
                          Higher.seeker(currentRange.toKey, Seek.Read, nextSeek)
                      }

                    case failed @ IO.Failure(_) =>
                      failed
                        .recoverToAsync(
                          Higher.seeker(key, currentSeek, nextSeek)
                        )
                  }

              case failure: IO.Failure[_] =>
                failure
                  .recoverToAsync(
                    Higher.seeker(key, currentSeek, nextSeek)
                  )
            }

          //if the input key is smaller than this Level's higher Range's fromKey.
          //0           (input key)
          //    10 - 20 (higher range)
          case _: KeyValue.ReadOnly.Range =>
            val nextStateID = nextWalker.stateID
            nextWalker.higher(key) match {
              case IO.Success(Some(next)) =>
                Higher(key, currentStash, Seek.Next.Stash(next, nextStateID))

              case IO.Success(None) =>
                Higher(key, currentStash, Seek.Next.Stop(nextStateID))

              case later @ IO.Deferred(_, _) =>
                later flatMap {
                  case Some(next) =>
                    Higher.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                  case None =>
                    Higher.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                }

              case failure: IO.Failure[_] =>
                failure
            }

          case _: ReadOnly.Fixed =>
            val nextStateID = nextWalker.stateID
            nextWalker.higher(key) match {
              case IO.Success(Some(next)) =>
                Higher(key, currentStash, Seek.Next.Stash(next, nextStateID))

              case IO.Success(None) =>
                Higher(key, currentStash, Seek.Next.Stop(nextStateID))

              case later @ IO.Deferred(_, _) =>
                later flatMap {
                  case Some(next) =>
                    Higher.seeker(key, currentStash, Seek.Next.Stash(next, nextStateID))

                  case None =>
                    Higher.seeker(key, currentStash, Seek.Next.Stop(nextStateID))
                }

              case failure: IO.Failure[_] =>
                failure
            }
        }

      case (Seek.Current.Stop, Seek.Read) =>
        val nextStateID = nextWalker.stateID
        nextWalker.higher(key) match {
          case IO.Success(Some(next)) =>
            Higher(key, currentSeek, Seek.Next.Stash(next, nextStateID))

          case IO.Success(None) =>
            Higher(key, currentSeek, Seek.Next.Stop(nextStateID))

          case later @ IO.Deferred(_, _) =>
            later flatMap {
              case Some(next) =>
                Higher.seeker(key, currentSeek, Seek.Next.Stash(next, nextStateID))

              case None =>
                Higher.seeker(key, currentSeek, Seek.Next.Stop(nextStateID))
            }

          case failure: IO.Failure[_] =>
            failure
        }

      /** *********************************************************
        * ******************                    *******************
        * ******************  Current on Stash  *******************
        * ******************                    *******************
        * *********************************************************/

      case (Seek.Current.Stop, Seek.Next.Stash(next, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Higher(key, currentSeek, Seek.Read)
        else if (next.hasTimeLeft())
          IO.Success(Some(next))
        else
          Higher(next.key, currentSeek, Seek.Read)

      case (Seek.Read, Seek.Next.Stash(_, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Higher(key, currentSeek, Seek.Read)
        else
          currentWalker.higher(key) match {
            case IO.Success(Some(current)) =>
              Higher(key, Seek.Current.Stash(current), nextSeek)

            case IO.Success(None) =>
              Higher(key, Seek.Current.Stop, nextSeek)

            case failure: IO.Failure[_] =>
              failure
                .recoverToAsync(
                  Higher.seeker(key, currentSeek, nextSeek)
                )
          }

      case (currentStash @ Seek.Current.Stash(current), nextStash @ Seek.Next.Stash(next, nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Higher(key, currentSeek, Seek.Read)
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
                        Higher(current.key, Seek.Read, Seek.Read)
                    }
                  case IO.Failure(error) =>
                    IO.Failure(error)
                      .recoverToAsync(
                        Higher.seeker(key, currentSeek, nextSeek)
                      )
                }
              //    2
              //      3  or  5
              else if (next.key > current.key)
                current match {
                  case put: ReadOnly.Put if put.hasTimeLeft() =>
                    IO.Success(Some(put))

                  //if it doesn't result in an unexpired put move forward.
                  case _ =>
                    Higher(current.key, Seek.Read, nextStash)
                }
              //    2
              //0
              else //else higher from next is smaller
                IO.Success(Some(next))

            /** *********************************************
              * *********************************************
              * ******************       ********************
              * ****************** RANGE ********************
              * ******************       ********************
              * *********************************************
              * *********************************************/
            case (current: KeyValue.ReadOnly.Range, next: KeyValue.ReadOnly.Fixed) =>
              //   10 - 20
              //1
              if (next.key < current.fromKey)
                IO.Success(Some(next))
              //10 - 20
              //10
              else if (next.key equiv current.fromKey)
                current.fetchFromOrElseRangeValue match {
                  case IO.Success(fromOrElseRangeValue) =>
                    FixedMerger(fromOrElseRangeValue.toMemory(current.fromKey), next) match {
                      case IO.Success(mergedCurrent) =>
                        mergedCurrent match {
                          case put: ReadOnly.Put if put.hasTimeLeft() =>
                            IO.Success(Some(put))
                          case _ =>
                            //do need to check if range is expired because if it was then
                            //next would not have been read from next level in the first place.
                            Higher(next.key, currentStash, Seek.Read)
                        }

                      case IO.Failure(error) =>
                        IO.Failure(error)
                          .recoverToAsync(
                            Higher.seeker(key, currentSeek, nextSeek)
                          )
                    }

                  case IO.Failure(error) =>
                    IO.Failure(error)
                      .recoverToAsync(
                        Higher.seeker(key, currentSeek, nextSeek)
                      )
                }

              //10  -  20
              //  11-19
              else if (next.key < current.toKey) //if the higher in next Level falls within the range.
                current.fetchFromAndRangeValue match {
                  //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                  case IO.Success((fromValue, rangeValue)) =>
                    higherFromValue(key, current.fromKey, fromValue) match {
                      case Some(fromValuePut) =>
                        IO.Success(Some(fromValuePut))

                      case None =>
                        FixedMerger(rangeValue.toMemory(next.key), next) match {
                          case IO.Success(mergedValue) =>
                            mergedValue match { //return applied value with next key-value as the current value.
                              case put: ReadOnly.Put if put.hasTimeLeft() =>
                                IO.Success(Some(put))

                              case _ =>
                                //fetch the next key keeping the current stash. next.key's higher is still current range
                                //since it's < range's toKey
                                Higher(next.key, currentStash, Seek.Read)
                            }

                          case failure: IO.Failure[_] =>
                            failure
                              .recoverToAsync(
                                Higher.seeker(key, currentSeek, nextSeek)
                              )
                        }
                    }

                  case failure: IO.Failure[_] =>
                    failure
                      .recoverToAsync(
                        Higher.seeker(key, currentSeek, nextSeek)
                      )
                }

              //10 - 20
              //     20 ----to----> ∞
              else //else if the higher in next Level does not fall within the range.
                current.fetchFromValue match {
                  //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                  case IO.Success(fromValue) =>
                    higherFromValue(key, current.fromKey, fromValue) match {
                      case somePut @ Some(_) =>
                        IO.Success(somePut)

                      case None =>
                        currentWalker.get(current.toKey) match {
                          case found @ IO.Success(Some(put)) =>
                            if (put.hasTimeLeft())
                              found
                            else
                              Higher(current.toKey, Seek.Read, nextStash)

                          case IO.Success(None) =>
                            Higher(current.toKey, Seek.Read, nextStash)

                          case later @ IO.Deferred(_, _) =>
                            later flatMap {
                              case Some(current) =>
                                Higher.seeker(key, Seek.Current.Stash(current), nextStash)

                              case None =>
                                Higher.seeker(current.toKey, Seek.Read, nextStash)
                            }

                          case failure: IO.Failure[_] =>
                            failure
                              .recoverToAsync(
                                Higher.seeker(key, currentSeek, nextSeek)
                              )
                        }
                    }

                  case failure: IO.Failure[_] =>
                    failure
                      .recoverToAsync(
                        Higher.seeker(key, currentSeek, nextSeek)
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
          Higher(key, currentSeek, Seek.Read)
        else
          currentWalker.higher(key) match {
            case IO.Success(Some(current)) =>
              Higher(key, Seek.Current.Stash(current), nextSeek)

            case IO.Success(None) =>
              Higher(key, Seek.Current.Stop, nextSeek)

            case failure: IO.Failure[_] =>
              failure
                .recoverToAsync(
                  Higher.seeker(key, currentSeek, nextSeek)
                )
          }

      case (currentStash @ Seek.Current.Stash(current), Seek.Next.Stop(nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Higher(key, currentSeek, Seek.Read)
        else
          current match {
            case current: KeyValue.ReadOnly.Put =>
              if (current.hasTimeLeft())
                IO.Success(Some(current))
              else
                Higher(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Remove =>
              Higher(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Update =>
              Higher(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.Function =>
              Higher(current.key, Seek.Read, nextSeek)

            case _: KeyValue.ReadOnly.PendingApply =>
              Higher(current.key, Seek.Read, nextSeek)

            case current: KeyValue.ReadOnly.Range =>
              current.fetchFromValue match {
                case IO.Success(fromValue) =>
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case somePut @ Some(_) =>
                      IO.Success(somePut)

                    case None =>
                      currentWalker.get(current.toKey) match {
                        case IO.Success(Some(put)) =>
                          Higher(key, Seek.Current.Stash(put), nextSeek)

                        case IO.Success(None) =>
                          Higher(current.toKey, Seek.Read, nextSeek)

                        case later @ IO.Deferred(_, _) =>
                          later flatMap {
                            case Some(put) =>
                              Higher.seeker(key, Seek.Current.Stash(put), nextSeek)

                            case None =>
                              Higher.seeker(current.toKey, Seek.Read, nextSeek)
                          }

                        case failure: IO.Failure[_] =>
                          failure
                      }
                  }

                case failure: IO.Failure[_] =>
                  failure
                    .recoverToAsync(
                      Higher.seeker(key, currentStash, nextSeek)
                    )
              }
          }

      case (Seek.Current.Stop, Seek.Next.Stop(nextStateID)) =>
        if (nextWalker.hasStateChanged(nextStateID))
          Higher(key, currentSeek, Seek.Read)
        else
          IO.none
    }
  }
}
