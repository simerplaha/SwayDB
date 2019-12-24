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
import swaydb.core.data.Value.FromValueOption
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.LevelSeek
import swaydb.core.merge._
import swaydb.core.segment.ReadState
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.SomeOrNone._

import scala.annotation.tailrec

private[core] object Lower {

  /**
   * Check and returns the FromValue if it's a valid lower key-value for the input key.
   */
  def lowerFromValue(key: Slice[Byte],
                     fromKey: Slice[Byte],
                     fromValue: FromValueOption)(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Memory.Put] = {
    import keyOrder._
    fromValue flatMapOptionS {
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
           readState: ReadState,
           currentSeek: Seek.Current,
           nextSeek: Seek.Next,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentWalker: CurrentWalker,
           nextWalker: NextWalker,
           functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] =
    Lower(key, readState, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  def seeker(key: Slice[Byte],
             readState: ReadState,
             currentSeek: Seek.Current,
             nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  currentWalker: CurrentWalker,
                                  nextWalker: NextWalker,
                                  functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] =
    Lower(key, readState, currentSeek, nextSeek)

  /**
   * May be use trampolining instead and split the matches into their own functions to reduce
   * repeated boilerplate code & if does not effect read performance or adds to GC workload.
   *
   * This and [[Higher]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
   * could help share this code and removing duplicates but only if there is no performance penalty.
   */
  @tailrec
  def apply(key: Slice[Byte],
            readState: ReadState,
            currentSeek: Seek.Current,
            nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 currentWalker: CurrentWalker,
                                 nextWalker: NextWalker,
                                 functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] = {

    import keyOrder._
    currentSeek match {
      case Seek.Current.Read(previousSegmentId) =>
        IO(currentWalker.lower(key, readState)) match {
          case IO.Right(LevelSeek.Some(segmentId, lower)) =>
            if (previousSegmentId == segmentId)
              Lower(key, readState, Seek.Current.Stash(segmentId, lower), nextSeek)
            else
              Lower(key, readState, Seek.Current.Stash(segmentId, lower), Seek.Next.Read)

          case IO.Right(LevelSeek.None) =>
            Lower(key, readState, Seek.Current.Stop, nextSeek)

          case failure @ IO.Left(_) =>
            failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
        }

      case currentStash @ Seek.Current.Stash(segmentId, current) =>
        nextSeek match {
          case Seek.Next.Read =>
            //decide if it's necessary to read the next Level or not.
            current match {
              //   19   (input key - exclusive)
              //10 - 20 (lower range from current Level)
              case currentRange: KeyValue.Range if key < currentRange.toKey =>
                IO(currentRange.fetchRangeValueUnsafe) match {
                  case IO.Right(rangeValue) =>
                    //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                    if (Value.hasTimeLeft(rangeValue))
                      nextWalker.lower(key, readState).toIO match {
                        case IO.Right(Some(next)) =>
                          Lower(key, readState, currentStash, Seek.Next.Stash(next))

                        case IO.Right(None) =>
                          Lower(key, readState, currentStash, Seek.Next.Stop)

                        case failure @ IO.Left(_) =>
                          failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                      }

                    //if the rangeValue is expired then check if fromValue is valid put else fetch from lower and merge.
                    else
                      IO(currentRange.fetchFromValueUnsafe) match {
                        case IO.Right(maybeFromValue) =>
                          //check if from value is a put before reading the next Level.
                          lowerFromValue(key, currentRange.fromKey, maybeFromValue) match {
                            case some @ Some(_) => //yes it is!
                              IO.Defer(some)

                            //if not, then fetch lower of key in next Level.
                            case None =>
                              nextWalker.lower(key, readState).toIO match {
                                case IO.Right(Some(next)) =>
                                  Lower(key, readState, currentStash, Seek.Next.Stash(next))

                                case IO.Right(None) =>
                                  Lower(key, readState, currentStash, Seek.Next.Stop)

                                case failure @ IO.Left(_) =>
                                  failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                              }
                          }

                        case failure @ IO.Left(_) =>
                          failure recoverTo Lower.seeker(key, readState, Seek.Current.Read(segmentId), nextSeek)
                      }

                  case failure @ IO.Left(_) =>
                    failure recoverTo Lower.seeker(key, readState, Seek.Current.Read(segmentId), nextSeek)
                }

              //     20 (input key - inclusive)
              //10 - 20 (lower range from current Level)
              case currentRange: KeyValue.Range if key equiv currentRange.toKey =>
                //lower level could also contain a toKey but toKey is exclusive to merge is not required but lower level is read is required.
                nextWalker.lower(key, readState).toIO match {
                  case IO.Right(Some(nextFromKey)) =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(nextFromKey))

                  case IO.Right(None) =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)

                  case failure @ IO.Left(_) =>
                    failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                }

              //             22 (input key)
              //    10 - 20     (lower range)
              case _: KeyValue.Range =>
                nextWalker.lower(key, readState).toIO match {
                  case IO.Right(Some(next)) =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(next))

                  case IO.Right(None) =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)

                  case failure @ IO.Left(_) =>
                    failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                }

              case _: KeyValue.Fixed =>
                nextWalker.lower(key, readState).toIO match {
                  case IO.Right(Some(next)) =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(next))

                  case IO.Right(None) =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)

                  case failure @ IO.Left(_) =>
                    failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                }
            }

          case nextStash @ Seek.Next.Stash(next) =>
            (current, next) match {

              /** **********************************************
               * ******************         *******************
               * ******************  Fixed  *******************
               * ******************         *******************
               * **********************************************/

              case (current: KeyValue.Fixed, next: KeyValue.Fixed) =>
                //    2
                //    2
                if (next.key equiv current.key)
                  IO(FixedMerger(current, next)) match {
                    case IO.Right(merged) =>
                      merged match {
                        case put: KeyValue.Put if put.hasTimeLeft() =>
                          IO.Defer(Some(put))

                        case _ =>
                          //if it doesn't result in an unexpired put move forward.
                          Lower(current.key, readState, Seek.Current.Read(segmentId), Seek.Next.Read)
                      }
                    case failure @ IO.Left(_) =>
                      failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                  }
                //      3  or  5 (current)
                //    1          (next)
                else if (current.key > next.key)
                  current match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      IO.Defer(Some(put))

                    //if it doesn't result in an unexpired put move forward.
                    case _ =>
                      Lower(current.key, readState, Seek.Current.Read(segmentId), nextStash)
                  }
                //0
                //    2
                else //else lower from next is the lowest.
                  IO.Defer(Some(next))

              /** *********************************************
               * *********************************************
               * ******************       ********************
               * ****************** RANGE ********************
               * ******************       ********************
               * *********************************************
               * *********************************************/
              case (current: KeyValue.Range, next: KeyValue.Fixed) =>
                //             22 (input)
                //10 - 20         (current)
                //     20 - 21    (next)
                if (next.key >= current.toKey)
                  IO.Defer(Some(next))

                //  11 ->   20 (input keys)
                //10   -    20 (lower range)
                //  11 -> 19   (lower possible keys from next)
                else if (next.key > current.fromKey)
                  IO(current.fetchRangeValueUnsafe) match {
                    case IO.Right(rangeValue) =>
                      IO(FixedMerger(rangeValue.toMemory(next.key), next)) match {
                        case IO.Right(mergedCurrent) =>
                          mergedCurrent match {
                            case put: KeyValue.Put if put.hasTimeLeft() =>
                              IO.Defer(Some(put))

                            case _ =>
                              //do need to check if range is expired because if it was then
                              //next would not have been read from next level in the first place.
                              Lower(next.key, readState, currentStash, Seek.Next.Read)
                          }

                        case failure @ IO.Left(_) =>
                          failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                  }

                //  11 ->   20 (input keys)
                //10   -    20 (lower range)
                //10
                else if (next.key equiv current.fromKey) //if the lower in next Level falls within the range.
                  IO(current.fetchFromOrElseRangeValueUnsafe) match {
                    //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                    case IO.Right(rangeValue) =>
                      lowerFromValue(key, current.fromKey, rangeValue) match {
                        case lowerPut @ Some(_) =>
                          IO.Defer(lowerPut)

                        //fromValue is not put, check if merging is required else return next.
                        case None =>
                          IO(FixedMerger(rangeValue.toMemory(next.key), next)) match {
                            case IO.Right(mergedValue) =>
                              mergedValue match { //return applied value with next key-value as the current value.
                                case put: Memory.Put if put.hasTimeLeft() =>
                                  IO.Defer(Some(put))

                                case _ =>
                                  Lower(next.key, readState, Seek.Current.Read(segmentId), Seek.Next.Read)
                              }

                            case failure @ IO.Left(_) =>
                              failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
                          }
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Lower.seeker(key, readState, Seek.Current.Read(segmentId), nextSeek)
                  }

                //       11 ->   20 (input keys)
                //     10   -    20 (lower range)
                //0->9
                else //else if the lower in next Level does not fall within the range.
                  IO(current.fetchFromValueUnsafe) match {
                    //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                    case IO.Right(fromValue) =>
                      lowerFromValue(key, current.fromKey, fromValue) match {
                        case somePut @ Some(_) =>
                          IO.Defer(somePut)

                        case None =>
                          Lower(current.fromKey, readState, Seek.Current.Read(segmentId), nextStash)
                      }

                    case failure @ IO.Left(_) =>
                      failure recoverTo Lower.seeker(key, readState, Seek.Current.Read(segmentId), nextSeek)
                  }
            }

          case Seek.Next.Stop =>
            current match {
              case current: KeyValue.Put =>
                if (current.hasTimeLeft())
                  IO.Defer(Some(current))
                else
                  Lower(current.key, readState, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.Remove =>
                Lower(current.key, readState, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.Update =>
                Lower(current.key, readState, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.Function =>
                Lower(current.key, readState, Seek.Current.Read(segmentId), nextSeek)

              case _: KeyValue.PendingApply =>
                Lower(current.key, readState, Seek.Current.Read(segmentId), nextSeek)

              case current: KeyValue.Range =>
                IO(current.fetchFromValueUnsafe) match {
                  case IO.Right(fromValue) =>
                    lowerFromValue(key, current.fromKey, fromValue) match {
                      case somePut @ Some(_) =>
                        IO.Defer(somePut)

                      case None =>
                        Lower(current.fromKey, readState, Seek.Current.Read(segmentId), nextSeek)
                    }

                  case failure @ IO.Left(_) =>
                    failure recoverTo Lower.seeker(key, readState, Seek.Current.Read(segmentId), nextSeek)
                }
            }
        }

      case Seek.Current.Stop =>
        nextSeek match {
          case Seek.Next.Read =>
            nextWalker.lower(key, readState).toIO match {
              case IO.Right(Some(next)) =>
                Lower(key, readState, currentSeek, Seek.Next.Stash(next))

              case IO.Right(None) =>
                Lower(key, readState, currentSeek, Seek.Next.Stop)

              case failure @ IO.Left(_) =>
                failure recoverTo Lower.seeker(key, readState, currentSeek, nextSeek)
            }

          case Seek.Next.Stash(next) =>
            if (next.hasTimeLeft())
              IO.Defer(Some(next))
            else
              Lower(next.key, readState, currentSeek, Seek.Next.Read)

          case Seek.Next.Stop =>
            IO.Defer.none
        }
    }
  }
}
