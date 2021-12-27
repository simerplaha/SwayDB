/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level.seek

import swaydb.core.level.LevelSeek
import swaydb.core.segment.CoreFunctionStore
import swaydb.core.segment.data.Value.FromValueOption
import swaydb.core.segment.data.merge._
import swaydb.core.segment.data.{KeyValue, Memory, Value}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}

import scala.annotation.tailrec

private[core] object Lower {

  /**
   * Check and returns the FromValue if it's a valid lower key-value for the input key.
   */
  def lowerFromValue(key: Slice[Byte],
                     fromKey: Slice[Byte],
                     fromValue: FromValueOption)(implicit keyOrder: KeyOrder[Slice[Byte]]): KeyValue.PutOption =
    fromValue.flatMapSomeS(KeyValue.Put.Null: KeyValue.PutOption) {
      fromValue =>
        if (keyOrder.lt(fromKey, key))
          fromValue.toMemory(fromKey) match {
            case put: Memory.Put if put.hasTimeLeft() =>
              put

            case _ =>
              KeyValue.Put.Null
          }
        else
          KeyValue.Put.Null
    }

  def seek(key: Slice[Byte],
           readState: ThreadReadState,
           currentSeek: Seek.Current,
           nextSeek: Seek.Next,
           keyOrder: KeyOrder[Slice[Byte]],
           timeOrder: TimeOrder[Slice[Byte]],
           currentWalker: CurrentWalker,
           nextWalker: NextWalker,
           functionStore: CoreFunctionStore): KeyValue.PutOption =
    Lower(key, readState, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  def seeker(key: Slice[Byte],
             readState: ThreadReadState,
             currentSeek: Seek.Current,
             nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  currentWalker: CurrentWalker,
                                  nextWalker: NextWalker,
                                  functionStore: CoreFunctionStore): KeyValue.PutOption =
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
            readState: ThreadReadState,
            currentSeek: Seek.Current,
            nextSeek: Seek.Next)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 currentWalker: CurrentWalker,
                                 nextWalker: NextWalker,
                                 functionStore: CoreFunctionStore): KeyValue.PutOption = {

    import keyOrder._
    currentSeek match {
      case Seek.Current.Read(previousSegmentId) =>
        currentWalker.lower(key, readState) match {
          case LevelSeek.Some(segmentNumber, lower) =>
            if (previousSegmentId == segmentNumber)
              Lower(key, readState, Seek.Current.Stash(segmentNumber, lower), nextSeek)
            else
              Lower(key, readState, Seek.Current.Stash(segmentNumber, lower), Seek.Next.Read)

          case LevelSeek.None =>
            Lower(key, readState, Seek.Current.Stop, nextSeek)
        }

      case currentStash @ Seek.Current.Stash(segmentNumber, current) =>
        nextSeek match {
          case Seek.Next.Read =>
            //decide if it's necessary to read the next Level or not.
            current match {
              //   19   (input key - exclusive)
              //10 - 20 (lower range from current Level)
              case currentRange: KeyValue.Range if key < currentRange.toKey =>
                //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                if (Value.hasTimeLeft(currentRange.fetchRangeValueUnsafe()))
                  nextWalker.lower(key, readState) match {
                    case next: KeyValue.Put =>
                      Lower(key, readState, currentStash, Seek.Next.Stash(next))

                    case KeyValue.Put.Null =>
                      Lower(key, readState, currentStash, Seek.Next.Stop)
                  }

                //if the rangeValue is expired then check if fromValue is valid put else fetch from lower and merge.
                else
                //check if from value is a put before reading the next Level.
                  lowerFromValue(key, currentRange.fromKey, currentRange.fetchFromValueUnsafe()) match {
                    case some: KeyValue.Put => //yes it is!
                      some

                    //if not, then fetch lower of key in next Level.
                    case KeyValue.Put.Null =>
                      nextWalker.lower(key, readState) match {
                        case next: KeyValue.Put =>
                          Lower(key, readState, currentStash, Seek.Next.Stash(next))

                        case KeyValue.Put.Null =>
                          Lower(key, readState, currentStash, Seek.Next.Stop)
                      }
                  }

              //     20 (input key - inclusive)
              //10 - 20 (lower range from current Level)
              case currentRange: KeyValue.Range if key equiv currentRange.toKey =>
                //lower level could also contain a toKey but toKey is exclusive to merge is not required but lower level is read is required.
                nextWalker.lower(key, readState) match {
                  case nextFromKey: KeyValue.Put =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(nextFromKey))

                  case KeyValue.Put.Null =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)
                }

              //             22 (input key)
              //    10 - 20     (lower range)
              case _: KeyValue.Range =>
                nextWalker.lower(key, readState) match {
                  case next: KeyValue.Put =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(next))

                  case KeyValue.Put.Null =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)
                }

              case _: KeyValue.Fixed =>
                nextWalker.lower(key, readState) match {
                  case next: KeyValue.Put =>
                    Lower(key, readState, currentStash, Seek.Next.Stash(next))

                  case KeyValue.Put.Null =>
                    Lower(key, readState, currentStash, Seek.Next.Stop)
                }
            }

          case nextStash @ Seek.Next.Stash(next) =>
            (current, next) match {

              /** **********************************************
               * ******************         *******************
               * ******************  Fixed  *******************
               * ******************         *******************
               * ********************************************* */

              case (current: KeyValue.Fixed, next: KeyValue.Fixed) =>
                //    2
                //    2
                if (next.key equiv current.key)
                  FixedMerger(current, next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _ =>
                      //if it doesn't result in an unexpired put move forward.
                      Lower(current.key, readState, Seek.Current.Read(segmentNumber), Seek.Next.Read)
                  }
                //      3  or  5 (current)
                //    1          (next)
                else if (current.key > next.key)
                  current match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    //if it doesn't result in an unexpired put move forward.
                    case _ =>
                      Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextStash)
                  }
                //0
                //    2
                else //else lower from next is the lowest.
                  next

              /** *********************************************
               * *********************************************
               * ******************       ********************
               * ****************** RANGE ********************
               * ******************       ********************
               * *********************************************
               * ******************************************** */
              case (current: KeyValue.Range, next: KeyValue.Fixed) =>
                //             22 (input)
                //10 - 20         (current)
                //     20 - 21    (next)
                if (next.key >= current.toKey)
                  next

                //  11 ->   20 (input keys)
                //10   -    20 (lower range)
                //  11 -> 19   (lower possible keys from next)
                else if (next.key > current.fromKey)
                  FixedMerger(current.fetchRangeValueUnsafe().toMemory(next.key), next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _ =>
                      //do need to check if range is expired because if it was then
                      //next would not have been read from next level in the first place.
                      Lower(next.key, readState, currentStash, Seek.Next.Read)
                  }

                //  11 ->   20 (input keys)
                //10   -    20 (lower range)
                //10
                else if (next.key equiv current.fromKey) { //if the lower in next Level falls within the range.
                  //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                  val rangeValue = current.fetchFromOrElseRangeValueUnsafe()
                  lowerFromValue(key, current.fromKey, rangeValue) match {
                    case lowerPut: KeyValue.Put =>
                      lowerPut

                    //fromValue is not put, check if merging is required else return next.
                    case KeyValue.Put.Null =>
                      FixedMerger(rangeValue.toMemory(next.key), next) match {
                        //return applied value with next key-value as the current value.
                        case put: Memory.Put if put.hasTimeLeft() =>
                          put

                        case _ =>
                          Lower(next.key, readState, Seek.Current.Read(segmentNumber), Seek.Next.Read)
                      }
                  }
                }

                //       11 ->   20 (input keys)
                //     10   -    20 (lower range)
                //0->9
                else //else if the lower in next Level does not fall within the range.
                //if fromValue is set check if it qualifies as the next highest orElse return lower of fromKey
                  lowerFromValue(key, current.fromKey, current.fetchFromValueUnsafe()) match {
                    case somePut: KeyValue.Put =>
                      somePut

                    case KeyValue.Put.Null =>
                      Lower(current.fromKey, readState, Seek.Current.Read(segmentNumber), nextStash)
                  }
            }

          case Seek.Next.Stop =>
            current match {
              case current: KeyValue.Put =>
                if (current.hasTimeLeft())
                  current
                else
                  Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Remove =>
                Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Update =>
                Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Function =>
                Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.PendingApply =>
                Lower(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case current: KeyValue.Range =>
                lowerFromValue(key, current.fromKey, current.fetchFromValueUnsafe()) match {
                  case put: KeyValue.Put =>
                    put

                  case KeyValue.Put.Null =>
                    Lower(current.fromKey, readState, Seek.Current.Read(segmentNumber), nextSeek)
                }
            }
        }

      case Seek.Current.Stop =>
        nextSeek match {
          case Seek.Next.Read =>
            nextWalker.lower(key, readState) match {
              case next: KeyValue.Put =>
                Lower(key, readState, currentSeek, Seek.Next.Stash(next))

              case KeyValue.Put.Null =>
                Lower(key, readState, currentSeek, Seek.Next.Stop)
            }

          case Seek.Next.Stash(next) =>
            if (next.hasTimeLeft())
              next
            else
              Lower(next.key, readState, currentSeek, Seek.Next.Read)

          case Seek.Next.Stop =>
            KeyValue.Put.Null
        }
    }
  }
}
