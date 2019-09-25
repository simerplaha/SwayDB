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

import swaydb.Error.Level.ExceptionHandler
import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.LevelSeek
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
           functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
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
                                          functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
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
                                 functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    //    println(s"Current walker: ${currentWalker.levelNumber} - ${key.readInt()}")

    currentSeek match {
      /** ****************************************************
       * ******************                *******************
       * ******************   Seek.Read    *******************
       * ******************                *******************
       * ****************************************************/

      case Seek.Current.Read(previousSegmentId) =>
        currentWalker.higher(key) match {
          case IO.Right(LevelSeek.Some(segmentId, higher)) =>
            if (previousSegmentId == segmentId)
              Higher(key, Seek.Current.Stash(segmentId, higher), nextSeek)
            else
              Higher(key, Seek.Current.Stash(segmentId, higher), Seek.Next.Read)

          case IO.Right(LevelSeek.None) =>
            Higher(key, Seek.Current.Stop, nextSeek)

          case failure @ IO.Left(_) =>
            failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
        }

      /** ********************************************************
       * ******************                   *******************
       * ******************  Current Stop     *******************
       * ******************                   *******************
       * ********************************************************/
      case Seek.Current.Stop =>
        nextSeek match {
          case Seek.Next.Read =>
            nextWalker.higher(key).toIO match {
              case IO.Right(Some(next)) =>
                Higher(key, currentSeek, Seek.Next.Stash(next))

              case IO.Right(None) =>
                Higher(key, currentSeek, Seek.Next.Stop)

              case failure @ IO.Left(_) =>
                failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
            }

          case Seek.Next.Stash(next) =>
            if (next.hasTimeLeft())
              IO.Defer(Some(next))
            else
              Higher(next.key, currentSeek, Seek.Next.Read)

          case Seek.Next.Stop =>
            IO.Defer.none
        }

      /** *********************************************************
       * ******************                     *******************
       * ******************    Current.Stash    *******************
       * ******************                     *******************
       * *********************************************************/

      case currentStash @ Seek.Current.Stash(segmentId, current) =>
        nextSeek match {
          case Seek.Next.Read =>
            //decide if it's necessary to read the next Level or not.
            current match {
              //10->19  (input keys)
              //10 - 20 (higher range from current Level)
              case currentRange: ReadOnly.Range if key >= currentRange.fromKey =>
                IO(currentRange.fetchRangeValueUnsafe) match {
                  case IO.Right(rangeValue) =>
                    //if the current range is active fetch the highest from next Level and return highest from both Levels.
                    if (Value.hasTimeLeft(rangeValue)) //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                      nextWalker.higher(key).toIO match {
                        case IO.Right(Some(next)) =>
                          Higher(key, currentStash, Seek.Next.Stash(next))

                        case IO.Right(None) =>
                          Higher(key, currentStash, Seek.Next.Stop)

                        case failure @ IO.Left(_) =>
                          failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                      }
                    else //if the rangeValue is expired then the higher is toKey or ceiling of toKey
                      currentWalker.get(currentRange.toKey).toIO match {
                        case IO.Right(Some(ceiling)) if ceiling.hasTimeLeft() =>
                          IO.Defer(Some(ceiling))

                        case IO.Right(_) =>
                          Higher(currentRange.toKey, Seek.Current.Read(segmentId), nextSeek)

                        case failed @ IO.Left(_) =>
                          failed recoverTo Higher.seeker(key, currentSeek, nextSeek)
                      }

                  case failure @ IO.Left(_) =>
                    failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                }

              //if the input key is smaller than this Level's higher Range's fromKey.
              //0           (input key)
              //    10 - 20 (higher range)
              case _: KeyValue.ReadOnly =>
                nextWalker.higher(key).toIO match {
                  case IO.Right(Some(next)) =>
                    Higher(key, currentStash, Seek.Next.Stash(next))

                  case IO.Right(None) =>
                    Higher(key, currentStash, Seek.Next.Stop)

                  case failure @ IO.Left(_) =>
                    failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                }
            }

          case Seek.Next.Stop =>
            current match {
              case current: KeyValue.ReadOnly.Put =>
                if (current.hasTimeLeft())
                  IO.Defer(Some(current))
                else
                  Higher(current.key, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.ReadOnly.Remove =>
                Higher(current.key, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.ReadOnly.Update =>
                Higher(current.key, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.ReadOnly.Function =>
                Higher(current.key, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.ReadOnly.PendingApply =>
                Higher(current.key, Seek.Current.Read(segmentId), nextSeek)

              case current: KeyValue.ReadOnly.Range =>
                IO(current.fetchFromValueUnsafe) match {
                  case IO.Right(fromValue) =>
                    higherFromValue(key, current.fromKey, fromValue) match {
                      case somePut @ Some(_) =>
                        IO.Defer(somePut)

                      case None =>
                        currentWalker.get(current.toKey).toIO match {
                          case IO.Right(some @ Some(put)) =>
                            if (put.hasTimeLeft())
                              IO.Defer(some)
                            else
                              Higher(current.toKey, Seek.Current.Read(segmentId), nextSeek)

                          case IO.Right(None) =>
                            Higher(current.toKey, Seek.Current.Read(segmentId), nextSeek)

                          case failure @ IO.Left(_) =>
                            failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                        }
                    }

                  case failure @ IO.Left(_) =>
                    failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                }
            }

          case nextStash @ Seek.Next.Stash(next) =>
            current match {

              /** **********************************************
               * ******************         *******************
               * ******************  Fixed  *******************
               * ******************         *******************
               * **********************************************/

              case current: KeyValue.ReadOnly.Fixed =>
                //    2
                //    2
                if (next.key equiv current.key)
                  FixedMerger(current, next) match {
                    case IO.Right(merged) =>
                      merged match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          IO.Defer(Some(put))

                        case _ =>
                          //if it doesn't result in an unexpired put move forward.
                          Higher(current.key, Seek.Current.Read(segmentId), Seek.Next.Read)
                      }
                    case failure @ IO.Left(_) =>
                      failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                  }
                //    2
                //      3  or  5
                else if (next.key > current.key)
                  current match {
                    case put: ReadOnly.Put if put.hasTimeLeft() =>
                      IO.Defer(Some(put))

                    //if it doesn't result in an unexpired put move forward.
                    case _ =>
                      Higher(current.key, Seek.Current.Read(segmentId), nextStash)
                  }
                //    2
                //0
                else //else higher from next is smaller
                  IO.Defer(Some(next))

              /** *********************************************
               * *********************************************
               * ******************       ********************
               * ****************** RANGE ********************
               * ******************       ********************
               * *********************************************
               * *********************************************/
              case current: KeyValue.ReadOnly.Range =>
                //   10 - 20
                //1
                if (next.key < current.fromKey)
                  IO.Defer(Some(next))
                //10 - 20
                //10
                else if (next.key equiv current.fromKey)
                  IO(current.fetchFromOrElseRangeValueUnsafe) match {
                    case IO.Right(fromOrElseRangeValue) =>
                      FixedMerger(fromOrElseRangeValue.toMemory(current.fromKey), next) match {
                        case IO.Right(mergedCurrent) =>
                          mergedCurrent match {
                            case put: ReadOnly.Put if put.hasTimeLeft() =>
                              IO.Defer(Some(put))
                            case _ =>
                              //do need to check if range is expired because if it was then
                              //next would not have been read from next level in the first place.
                              Higher(next.key, currentStash, Seek.Next.Read)
                          }

                        case failure @ IO.Left(_) =>
                          failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Higher.seeker(key, Seek.Current.Read(segmentId), nextSeek)
                  }

                //10  -  20
                //  11-19
                else if (next.key < current.toKey) //if the higher in next Level falls within the range.
                  IO(current.fetchFromAndRangeValueUnsafe) match {
                    //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                    case IO.Right((fromValue, rangeValue)) =>
                      higherFromValue(key, current.fromKey, fromValue) match {
                        case Some(fromValuePut) =>
                          IO.Defer(Some(fromValuePut))

                        case None =>
                          FixedMerger(rangeValue.toMemory(next.key), next) match {
                            case IO.Right(mergedValue) =>
                              mergedValue match { //return applied value with next key-value as the current value.
                                case put: ReadOnly.Put if put.hasTimeLeft() =>
                                  IO.Defer(Some(put))

                                case _ =>
                                  //fetch the next key keeping the current stash. next.key's higher is still current range
                                  //since it's < range's toKey
                                  Higher(next.key, currentStash, Seek.Next.Read)
                              }

                            case failure @ IO.Left(_) =>
                              failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                          }
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Higher.seeker(key, Seek.Current.Read(segmentId), nextSeek)
                  }

                //10 - 20
                //     20 ----to----> ∞
                else //else if the higher in next Level does not fall within the range.
                  IO(current.fetchFromValueUnsafe) match {
                    //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                    case IO.Right(fromValue) =>
                      higherFromValue(key, current.fromKey, fromValue) match {
                        case somePut @ Some(_) =>
                          IO.Defer(somePut)

                        case None =>
                          currentWalker.get(current.toKey).toIO match {
                            case IO.Right(some @ Some(put)) =>
                              if (put.hasTimeLeft())
                                IO.Defer(some)
                              else
                                Higher(current.toKey, Seek.Current.Read(segmentId), nextStash)

                            case IO.Right(None) =>
                              Higher(current.toKey, Seek.Current.Read(segmentId), nextStash)

                            case failure @ IO.Left(_) =>
                              failure recoverTo Higher.seeker(key, currentSeek, nextSeek)
                          }
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Higher.seeker(key, Seek.Current.Read(segmentId), nextSeek)
                  }
            }
        }
    }
  }
}
