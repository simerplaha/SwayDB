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

package swaydb.core.finder

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.finder.Seek.Stash
import swaydb.core.function.FunctionStore
import swaydb.core.merge._
import swaydb.core.util.TryUtil
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[swaydb] sealed trait CurrentSeek
private[swaydb] sealed trait NextSeek

private[swaydb] object Seek {
  sealed trait Stop extends CurrentSeek with NextSeek
  case object Stop extends Stop

  sealed trait Next extends CurrentSeek with NextSeek
  case object Next extends Next

  object Stash {
    case class Current(current: KeyValue.ReadOnly.SegmentResponse) extends CurrentSeek
    case class Next(next: KeyValue.ReadOnly.Put) extends NextSeek
  }
}

private[core] object Higher {

  private def higherFromValue(key: Slice[Byte],
                              fromKey: Slice[Byte],
                              fromValue: Option[Value.FromValue])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Memory.Put] = {
    import keyOrder._
    if (key < fromKey)
      fromValue flatMap {
        fromValue =>
          fromValue.toMemory(fromKey) match {
            case put: Memory.Put if put.hasTimeLeft() =>
              Some(put)

            case _ =>
              None
          }
      }
    else
      None
  }

  def seek(key: Slice[Byte],
           currentSeek: CurrentSeek,
           nextSeek: NextSeek,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentReader: CurrentFinder,
           nextReader: NextFinder,
           functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] =
    Higher(key, currentSeek, nextSeek)(keyOrder, timeOrder, currentReader, nextReader, functionStore)

  /**
    * May be use trampolining instead and split the matches into their own functions to reduce
    * repeated boilerplate code & if does not effect read performance or adds to GC workload.
    */
  @tailrec
  def apply(key: Slice[Byte],
            currentSeek: CurrentSeek,
            nextSeek: NextSeek)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                timeOrder: TimeOrder[Slice[Byte]],
                                currentFinder: CurrentFinder,
                                nextFinder: NextFinder,
                                functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    (currentSeek, nextSeek) match {
      /** ********************************************************
        * ******************                   *******************
        * ******************  Current on Next  *******************
        * ******************                   *******************
        * ********************************************************/

      case (Seek.Next, Seek.Next) =>
        currentFinder.higher(key) match {
          case Success(Some(higher)) =>
            Higher(key, Stash.Current(higher), nextSeek)

          case Success(None) =>
            Higher(key, Seek.Stop, nextSeek)

          case Failure(exception) =>
            Failure(exception)
        }

      case (currentStash @ Stash.Current(current), Seek.Next) =>
        //decide if it's necessary to read the next Level or not.
        current match {
          //10->19  (input keys)
          //10 - 20 (higher range from current Level)
          case currentRange: ReadOnly.Range if key >= currentRange.fromKey =>
            currentRange.fetchRangeValue match {
              case Success(rangeValue) =>
                //if the current range is active fetch the highest from next Level and return highest from both Levels.
                if (Value.hasTimeLeft(rangeValue))
                //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  nextFinder.higher(key) match {
                    case Success(Some(next)) =>
                      Higher(key, currentStash, Stash.Next(next))

                    case Success(None) =>
                      Higher(key, currentStash, Seek.Stop)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                else //if the rangeValue is expired then the higher is ceiling of toKey
                  currentFinder.get(currentRange.toKey) match {
                    case Success(Some(ceiling)) if ceiling.hasTimeLeft() =>
                      Success(Some(ceiling))

                    case Success(_) =>
                      Higher(currentRange.toKey, Seek.Next, nextSeek)

                    case failed @ Failure(_) =>
                      failed
                  }

              case Failure(exception) =>
                Failure(exception)
            }

          //if the input key is smaller than this Level's higher Range's fromKey.
          //0           (input key)
          //    10 - 20 (higher range)
          case _: KeyValue.ReadOnly.Range =>
            nextFinder.higher(key) match {
              case Success(Some(next)) =>
                Higher(key, currentStash, Stash.Next(next))

              case Success(None) =>
                Higher(key, currentStash, Seek.Stop)

              case Failure(exception) =>
                Failure(exception)
            }

          case _: ReadOnly.Fixed =>
            nextFinder.higher(key) match {
              case Success(Some(next)) =>
                Higher(key, currentStash, Stash.Next(next))

              case Success(None) =>
                Higher(key, currentStash, Seek.Stop)

              case Failure(exception) =>
                Failure(exception)
            }
        }

      case (Seek.Stop, Seek.Next) =>
        nextFinder.higher(key) match {
          case Success(Some(next)) =>
            Higher(key, currentSeek, Stash.Next(next))

          case Success(None) =>
            Higher(key, currentSeek, Seek.Stop)

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
          Higher(next.key, currentSeek, Seek.Next)

      case (Seek.Next, Stash.Next(_)) =>
        currentFinder.higher(key) match {
          case Success(Some(current)) =>
            Higher(key, Stash.Current(current), nextSeek)

          case Success(None) =>
            Higher(key, Seek.Stop, nextSeek)

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
                      Higher(current.key, Seek.Next, Seek.Next)
                  }
                case Failure(exception) =>
                  Failure(exception)
              }
            //    2
            //      3  or  5
            else if (next.key > current.key)
              current match {
                case put: ReadOnly.Put if put.hasTimeLeft() =>
                  Success(Some(put))

                //if it doesn't result in an unexpired put move forward.
                case _ =>
                  Higher(current.key, Seek.Next, nextStash)
              }
            //    2
            //0
            else //else higher from next is smaller
              Success(Some(next))

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
              Success(Some(next))
            //10 - 20
            //10
            else if (next.key equiv current.fromKey)
              current.fetchFromOrElseRangeValue match {
                case Success(fromOrElseRangeValue) =>
                  FixedMerger(fromOrElseRangeValue.toMemory(current.fromKey), next) match {
                    case Success(mergedCurrent) =>
                      mergedCurrent match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          Success(Some(put))
                        case _ =>
                          //do need to check if range is expired because if it was then
                          //next would not have been read from next level in the first place.
                          Higher(next.key, currentStash, Seek.Next)

                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //10  -  20
            //  11-19
            else if (next.key < current.toKey) //if the higher in next Level falls within the range.
              current.fetchFromAndRangeValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                case Success((fromValue, rangeValue)) =>
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case Some(fromValuePut) =>
                      Success(Some(fromValuePut))

                    case None =>
                      FixedMerger(rangeValue.toMemory(next.key), next) match {
                        case Success(mergedValue) =>
                          mergedValue match { //return applied value with next key-value as the current value.
                            case put: Memory.Put if put.hasTimeLeft() =>
                              Success(Some(put))

                            case _ =>
                              //fetch the next key keeping the current stash. next.key's higher is still current range
                              //since it's < range's toKey
                              Higher(next.key, currentStash, Seek.Next)
                          }

                        case Failure(exception) =>
                          Failure(exception)
                      }
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //10 - 20
            //     20 ----to----> ∞
            else //else if the higher in next Level does not fall within the range.
              current.fetchFromValue match {
                //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                case Success(fromValue) =>
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case somePut @ Some(_) =>
                      Success(somePut)

                    case None =>
                      currentFinder.get(current.toKey) match {
                        case found @ Success(Some(put)) =>
                          if (put.hasTimeLeft())
                            found
                          else
                            Higher(current.toKey, Seek.Next, nextStash)

                        case Success(None) =>
                          Higher(current.toKey, Seek.Next, nextStash)

                        case Failure(exception) =>
                          Failure(exception)
                      }
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
        currentFinder.higher(key) match {
          case Success(Some(current)) =>
            Higher(key, Stash.Current(current), nextSeek)

          case Success(None) =>
            Higher(key, Seek.Stop, nextSeek)

          case Failure(exception) =>
            Failure(exception)
        }

      case (Stash.Current(current), Seek.Stop) =>
        current match {
          case current: KeyValue.ReadOnly.Put =>
            if (current.hasTimeLeft())
              Success(Some(current))
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
              case Success(fromValue) =>
                higherFromValue(key, current.fromKey, fromValue) match {
                  case somePut @ Some(_) =>
                    Success(somePut)

                  case None =>
                    currentFinder.get(current.toKey) match {
                      case Success(Some(put)) =>
                        Higher(key, Stash.Current(put), nextSeek)

                      case Success(None) =>
                        Higher(current.toKey, Seek.Next, nextSeek)

                      case Failure(exception) =>
                        Failure(exception)
                    }
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
