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

package swaydb.core.seek

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.seek.Seek.Stash
import swaydb.core.function.FunctionStore
import swaydb.core.merge._
import swaydb.core.util.TryUtil
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[core] object Lower {

  private def lowerFromValue(key: Slice[Byte],
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
           currentSeek: CurrentSeek,
           nextSeek: NextSeek,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentWalker: CurrentWalker,
           nextWalker: NextWalker,
           functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] =
    Lower(key, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  /**
    * May be use trampolining instead and split the matches into their own functions to reduce
    * repeated boilerplate code & if does not effect read performance or adds to GC workload.
    *
    * This and [[Higher]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
    * could help share this code and removing duplicates but only if there is no performance penalty.
    */
  @tailrec
  def apply(key: Slice[Byte],
            currentSeek: CurrentSeek,
            nextSeek: NextSeek)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentWalker: CurrentWalker,
                                nextWalker: NextWalker,
                                functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    (currentSeek, nextSeek) match {
      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Next  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Next, Seek.Next) =>
        currentWalker.lower(key) match {
          case Success(Some(lower)) =>
            Lower(key, Stash.Current(lower), nextSeek)

          case Success(None) =>
            Lower(key, Seek.Stop, nextSeek)

          case Failure(exception) =>
            Failure(exception)
        }

      case (currentStash @ Stash.Current(current), Seek.Next) =>
        //decide if it's necessary to read the next Level or not.
        current match {
          //   19   (input key - exclusive)
          //10 - 20 (lower range from current Level)
          case currentRange: ReadOnly.Range if key < currentRange.toKey =>
            currentRange.fetchRangeValue match {
              case Success(rangeValue) =>
                //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                if (Value.hasTimeLeft(rangeValue))
                  nextWalker.lower(key) match {
                    case Success(Some(next)) =>
                      Lower(key, currentStash, Stash.Next(next))

                    case Success(None) =>
                      Lower(key, currentStash, Seek.Stop)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                //if the rangeValue is expired then check if fromValue is valid put else fetch from lower and merge.
                else
                  currentRange.fetchFromValue match {
                    case Success(maybeFromValue) =>
                      //check if from value is a put before reading the next Level.
                      lowerFromValue(key, currentRange.fromKey, maybeFromValue) match {
                        case some @ Some(_) => //yes it is!
                          Success(some)

                        //if not, then fetch lower of key in next Level.
                        case None =>
                          nextWalker.lower(key) match {
                            case Success(Some(next)) =>
                              Lower(key, currentStash, Stash.Next(next))

                            case Success(None) =>
                              Lower(key, currentStash, Seek.Stop)

                            case Failure(exception) =>
                              Failure(exception)
                          }
                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }

              case Failure(exception) =>
                Failure(exception)
            }

          //     20 (input key - inclusive)
          //10 - 20 (lower range from current Level)
          case currentRange: ReadOnly.Range if key equiv currentRange.toKey =>
            //lower level could also contain a toKey but toKey is exclusive to merge is not required but lower level is read is required.
            nextWalker.lower(key) match {
              case Success(Some(nextFromKey)) =>
                Lower(key, currentStash, Stash.Next(nextFromKey))

              case Success(None) =>
                Lower(key, currentStash, Seek.Stop)

              case Failure(exception) =>
                Failure(exception)

            }

          //             22 (input key)
          //    10 - 20     (lower range)
          case _: KeyValue.ReadOnly.Range =>
            nextWalker.lower(key) match {
              case Success(Some(next)) =>
                Lower(key, currentStash, Stash.Next(next))

              case Success(None) =>
                Lower(key, currentStash, Seek.Stop)

              case Failure(exception) =>
                Failure(exception)
            }

          case _: ReadOnly.Fixed =>
            nextWalker.lower(key) match {
              case Success(Some(next)) =>
                Lower(key, currentStash, Stash.Next(next))

              case Success(None) =>
                Lower(key, currentStash, Seek.Stop)

              case Failure(exception) =>
                Failure(exception)
            }
        }

      case (Seek.Stop, Seek.Next) =>
        nextWalker.lower(key) match {
          case Success(Some(next)) =>
            Lower(key, currentSeek, Stash.Next(next))

          case Success(None) =>
            Lower(key, currentSeek, Seek.Stop)

          case Failure(exception) =>
            Failure(exception)
        }

      /** *********************************************************
        * ******************                    *******************
        * ******************  Current on Stash  *******************
        * ******************                    *******************
        * *********************************************************/

      case (Seek.Stop, Stash.Next(next)) =>
        if (next.hasTimeLeft())
          Success(Some(next))
        else
          Lower(next.key, currentSeek, Seek.Next)

      case (Seek.Next, Stash.Next(_)) =>
        currentWalker.lower(key) match {
          case Success(Some(current)) =>
            Lower(key, Stash.Current(current), nextSeek)

          case Success(None) =>
            Lower(key, Seek.Stop, nextSeek)

          case Failure(exception) =>
            Failure(exception)
        }

      case (currentStash @ Stash.Current(current), nextStash @ Stash.Next(next)) =>
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
                case Success(merged) =>
                  merged match {
                    case put: ReadOnly.Put if put.hasTimeLeft() =>
                      Success(Some(put))

                    case _ =>
                      //if it doesn't result in an unexpired put move forward.
                      Lower(current.key, Seek.Next, Seek.Next)
                  }
                case Failure(exception) =>
                  Failure(exception)
              }
            //      3  or  5 (current)
            //    1          (next)
            else if (current.key > next.key)
              current match {
                case put: ReadOnly.Put if put.hasTimeLeft() =>
                  Success(Some(put))

                //if it doesn't result in an unexpired put move forward.
                case _ =>
                  Lower(current.key, Seek.Next, nextStash)
              }
            //0
            //    2
            else //else lower from next is the lowest.
              Success(Some(next))

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
              Success(Some(next))

            //  11 ->   20 (input keys)
            //10   -    20 (lower range)
            //  11 -> 19   (lower possible keys from next)
            else if (next.key > current.fromKey)
              current.fetchRangeValue match {
                case Success(rangeValue) =>
                  FixedMerger(rangeValue.toMemory(next.key), next) match {
                    case Success(mergedCurrent) =>
                      mergedCurrent match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          Success(Some(put))

                        case _ =>
                          //do need to check if range is expired because if it was then
                          //next would not have been read from next level in the first place.
                          Lower(next.key, currentStash, Seek.Next)

                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //  11 ->   20 (input keys)
            //10   -    20 (lower range)
            //10
            else if (next.key equiv current.fromKey) //if the lower in next Level falls within the range.
              current.fetchFromValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                case Success(maybeFromValue) =>
                  lowerFromValue(key, current.fromKey, maybeFromValue) match {
                    case lowerPut @ Some(_) =>
                      Success(lowerPut)

                    //fromValue is not put, check if merging is required else return next.
                    case None =>
                      val mergeValue: Try[ReadOnly.Fixed] =
                        maybeFromValue
                          .map(fromValue => FixedMerger(fromValue.toMemory(next.key), next))
                          .getOrElse(Success(next))

                      mergeValue match {
                        case Success(mergedValue) =>
                          mergedValue match { //return applied value with next key-value as the current value.
                            case put: Memory.Put if put.hasTimeLeft() =>
                              Success(Some(put))

                            case _ =>
                              Lower(next.key, Seek.Next, Seek.Next)
                          }

                        case Failure(exception) =>
                          Failure(exception)
                      }
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //       11 ->   20 (input keys)
            //     10   -    20 (lower range)
            //0->9
            else //else if the lower in next Level does not fall within the range.
              current.fetchFromValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                case Success(fromValue) =>
                  lowerFromValue(key, current.fromKey, fromValue) match {
                    case somePut @ Some(_) =>
                      Success(somePut)

                    case None =>
                      Lower(current.fromKey, Seek.Next, nextStash)
                  }

                case Failure(exception) =>
                  Failure(exception)
              }
        }

      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Stop  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Next, Seek.Stop) =>
        currentWalker.lower(key) match {
          case Success(Some(current)) =>
            Lower(key, Stash.Current(current), nextSeek)

          case Success(None) =>
            Lower(key, Seek.Stop, nextSeek)

          case Failure(exception) =>
            Failure(exception)
        }

      case (Stash.Current(current), Seek.Stop) =>
        current match {
          case current: KeyValue.ReadOnly.Put =>
            if (current.hasTimeLeft())
              Success(Some(current))
            else
              Lower(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Remove =>
            Lower(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Update =>
            Lower(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Function =>
            Lower(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.PendingApply =>
            Lower(current.key, Seek.Next, nextSeek)

          case current: KeyValue.ReadOnly.Range =>
            current.fetchFromValue match {
              case Success(fromValue) =>
                lowerFromValue(key, current.fromKey, fromValue) match {
                  case somePut @ Some(_) =>
                    Success(somePut)

                  case None =>
                    Lower(current.fromKey, Seek.Next, nextSeek)
                }

              case Failure(exception) =>
                Failure(exception)
            }
        }

      case (Seek.Stop, Seek.Stop) =>
        TryUtil.successNone
    }
  }
}
