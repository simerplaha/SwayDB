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
import swaydb.data.io.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.seek.Seek.Stash
import swaydb.core.function.FunctionStore
import swaydb.core.merge._

import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

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
           currentSeek: CurrentSeek,
           nextSeek: NextSeek,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentWalker: CurrentWalker,
           nextWalker: NextWalker,
           functionStore: FunctionStore): IO[Option[KeyValue.ReadOnly.Put]] =
    Higher(key, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  /**
    * May be use trampolining instead and split the matches into their own functions to reduce
    * repeated boilerplate code & if does not effect read performance or adds to GC workload.
    *
    * This and [[Lower]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
    * could help share this code and removing duplicates but only if there is no performance penalty.
    */
  @tailrec
  def apply(key: Slice[Byte],
            currentSeek: CurrentSeek,
            nextSeek: NextSeek)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentWalker: CurrentWalker,
                                nextWalker: NextWalker,
                                functionStore: FunctionStore): IO[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    (currentSeek, nextSeek) match {
      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Next  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Next, Seek.Next) =>
        currentWalker.higher(key) match {
          case IO.Sync(Some(higher)) =>
            Higher(key, Stash.Current(higher), nextSeek)

          case IO.Sync(None) =>
            Higher(key, Seek.Stop, nextSeek)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case (currentStash @ Stash.Current(current), Seek.Next) =>
        //decide if it's necessary to read the next Level or not.
        current match {
          //10->19  (input keys)
          //10 - 20 (higher range from current Level)
          case currentRange: ReadOnly.Range if key >= currentRange.fromKey =>
            currentRange.fetchRangeValue match {
              case IO.Sync(rangeValue) =>
                //if the current range is active fetch the highest from next Level and return highest from both Levels.
                if (Value.hasTimeLeft(rangeValue))
                //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  nextWalker.higher(key) match {
                    case IO.Sync(Some(next)) =>
                      Higher(key, currentStash, Stash.Next(next))

                    case IO.Sync(None) =>
                      Higher(key, currentStash, Seek.Stop)

                    case IO.Failure(exception) =>
                      IO.Failure(exception)
                  }

                else //if the rangeValue is expired then the higher is ceiling of toKey
                  currentWalker.get(currentRange.toKey) match {
                    case IO.Sync(Some(ceiling)) if ceiling.hasTimeLeft() =>
                      IO.Sync(Some(ceiling))

                    case IO.Sync(_) =>
                      Higher(currentRange.toKey, Seek.Next, nextSeek)

                    case failed @ IO.Failure(_) =>
                      failed
                  }

              case IO.Failure(exception) =>
                IO.Failure(exception)
            }

          //if the input key is smaller than this Level's higher Range's fromKey.
          //0           (input key)
          //    10 - 20 (higher range)
          case _: KeyValue.ReadOnly.Range =>
            nextWalker.higher(key) match {
              case IO.Sync(Some(next)) =>
                Higher(key, currentStash, Stash.Next(next))

              case IO.Sync(None) =>
                Higher(key, currentStash, Seek.Stop)

              case IO.Failure(exception) =>
                IO.Failure(exception)
            }

          case _: ReadOnly.Fixed =>
            nextWalker.higher(key) match {
              case IO.Sync(Some(next)) =>
                Higher(key, currentStash, Stash.Next(next))

              case IO.Sync(None) =>
                Higher(key, currentStash, Seek.Stop)

              case IO.Failure(exception) =>
                IO.Failure(exception)
            }
        }

      case (Seek.Stop, Seek.Next) =>
        nextWalker.higher(key) match {
          case IO.Sync(Some(next)) =>
            Higher(key, currentSeek, Stash.Next(next))

          case IO.Sync(None) =>
            Higher(key, currentSeek, Seek.Stop)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      /** *********************************************************
        * ******************                    *******************
        * ******************  Current on Stash  *******************
        * ******************                    *******************
        * *********************************************************/

      case (Seek.Stop, Stash.Next(next)) =>
        if (next.hasTimeLeft())
          IO.Sync(Some(next))
        else
          Higher(next.key, currentSeek, Seek.Next)

      case (Seek.Next, Stash.Next(_)) =>
        currentWalker.higher(key) match {
          case IO.Sync(Some(current)) =>
            Higher(key, Stash.Current(current), nextSeek)

          case IO.Sync(None) =>
            Higher(key, Seek.Stop, nextSeek)

          case IO.Failure(exception) =>
            IO.Failure(exception)
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
                case IO.Sync(merged) =>
                  merged match {
                    case put: ReadOnly.Put if put.hasTimeLeft() =>
                      IO.Sync(Some(put))

                    case _ =>
                      //if it doesn't result in an unexpired put move forward.
                      Higher(current.key, Seek.Next, Seek.Next)
                  }
                case IO.Failure(exception) =>
                  IO.Failure(exception)
              }
            //    2
            //      3  or  5
            else if (next.key > current.key)
              current match {
                case put: ReadOnly.Put if put.hasTimeLeft() =>
                  IO.Sync(Some(put))

                //if it doesn't result in an unexpired put move forward.
                case _ =>
                  Higher(current.key, Seek.Next, nextStash)
              }
            //    2
            //0
            else //else higher from next is smaller
              IO.Sync(Some(next))

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
              IO.Sync(Some(next))
            //10 - 20
            //10
            else if (next.key equiv current.fromKey)
              current.fetchFromOrElseRangeValue match {
                case IO.Sync(fromOrElseRangeValue) =>
                  FixedMerger(fromOrElseRangeValue.toMemory(current.fromKey), next) match {
                    case IO.Sync(mergedCurrent) =>
                      mergedCurrent match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          IO.Sync(Some(put))
                        case _ =>
                          //do need to check if range is expired because if it was then
                          //next would not have been read from next level in the first place.
                          Higher(next.key, currentStash, Seek.Next)

                      }

                    case IO.Failure(exception) =>
                      IO.Failure(exception)
                  }

                case IO.Failure(exception) =>
                  IO.Failure(exception)
              }

            //10  -  20
            //  11-19
            else if (next.key < current.toKey) //if the higher in next Level falls within the range.
              current.fetchFromAndRangeValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                case IO.Sync((fromValue, rangeValue)) =>
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case Some(fromValuePut) =>
                      IO.Sync(Some(fromValuePut))

                    case None =>
                      FixedMerger(rangeValue.toMemory(next.key), next) match {
                        case IO.Sync(mergedValue) =>
                          mergedValue match { //return applied value with next key-value as the current value.
                            case put: Memory.Put if put.hasTimeLeft() =>
                              IO.Sync(Some(put))

                            case _ =>
                              //fetch the next key keeping the current stash. next.key's higher is still current range
                              //since it's < range's toKey
                              Higher(next.key, currentStash, Seek.Next)
                          }

                        case IO.Failure(exception) =>
                          IO.Failure(exception)
                      }
                  }

                case IO.Failure(exception) =>
                  IO.Failure(exception)
              }

            //10 - 20
            //     20 ----to----> ∞
            else //else if the higher in next Level does not fall within the range.
              current.fetchFromValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                case IO.Sync(fromValue) =>
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case somePut @ Some(_) =>
                      IO.Sync(somePut)

                    case None =>
                      currentWalker.get(current.toKey) match {
                        case found @ IO.Sync(Some(put)) =>
                          if (put.hasTimeLeft())
                            found
                          else
                            Higher(current.toKey, Seek.Next, nextStash)

                        case IO.Sync(None) =>
                          Higher(current.toKey, Seek.Next, nextStash)

                        case IO.Failure(exception) =>
                          IO.Failure(exception)
                      }
                  }

                case IO.Failure(exception) =>
                  IO.Failure(exception)
              }
        }

      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Stop  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Next, Seek.Stop) =>
        currentWalker.higher(key) match {
          case IO.Sync(Some(current)) =>
            Higher(key, Stash.Current(current), nextSeek)

          case IO.Sync(None) =>
            Higher(key, Seek.Stop, nextSeek)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case (Stash.Current(current), Seek.Stop) =>
        current match {
          case current: KeyValue.ReadOnly.Put =>
            if (current.hasTimeLeft())
              IO.Sync(Some(current))
            else
              Higher(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Remove =>
            Higher(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Update =>
            Higher(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.Function =>
            Higher(current.key, Seek.Next, nextSeek)

          case _: KeyValue.ReadOnly.PendingApply =>
            Higher(current.key, Seek.Next, nextSeek)

          case current: KeyValue.ReadOnly.Range =>
            current.fetchFromValue match {
              case IO.Sync(fromValue) =>
                higherFromValue(key, current.fromKey, fromValue) match {
                  case somePut @ Some(_) =>
                    IO.Sync(somePut)

                  case None =>
                    currentWalker.get(current.toKey) match {
                      case IO.Sync(Some(put)) =>
                        Higher(key, Stash.Current(put), nextSeek)

                      case IO.Sync(None) =>
                        Higher(current.toKey, Seek.Next, nextSeek)

                      case IO.Failure(exception) =>
                        IO.Failure(exception)
                    }
                }

              case IO.Failure(exception) =>
                IO.Failure(exception)
            }
        }

      case (Seek.Stop, Seek.Stop) =>
        IO.successNone
    }
  }
}
