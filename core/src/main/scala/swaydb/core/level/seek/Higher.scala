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

import swaydb.core.data.Value.FromValueOption
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.level.LevelSeek
import swaydb.core.merge._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice

import scala.annotation.tailrec

private[core] object Higher {

  /**
   * Check and returns the FromValue if it's a valid higher key-value for the input key
   */
  def higherFromValue(key: Slice[Byte],
                      fromKey: Slice[Byte],
                      fromValue: FromValueOption)(implicit keyOrder: KeyOrder[Slice[Byte]]): KeyValue.PutOption =
    fromValue.flatMapSomeS(KeyValue.Put.Null: KeyValue.PutOption) {
      fromValue =>
        if (keyOrder.gt(fromKey, key))
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
           functionStore: FunctionStore): KeyValue.PutOption =
    Higher(key, readState, currentSeek, nextSeek)(keyOrder, timeOrder, currentWalker, nextWalker, functionStore)

  /**
   * May be use trampolining instead and split the matches into their own functions to reduce
   * repeated boilerplate code & if does not effect read performance or adds to GC workload.
   *
   * This and [[Lower]] share a lot of the same code for certain [[Seek]] steps. Again trampolining
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
                                 functionStore: FunctionStore): KeyValue.PutOption = {
    import keyOrder._

    //    println(s"Current walker: ${currentWalker.levelNumber} - ${key.readInt()}")

    currentSeek match {
      /** ****************************************************
       * ******************                *******************
       * ******************   Seek.Read    *******************
       * ******************                *******************
       * *************************************************** */

      case Seek.Current.Read(previousSegmentId) =>
        currentWalker.higher(key, readState) match {
          case LevelSeek.Some(segmentNumber, higher) =>
            if (previousSegmentId == segmentNumber)
              Higher(key, readState, Seek.Current.Stash(segmentNumber, higher), nextSeek)
            else
              Higher(key, readState, Seek.Current.Stash(segmentNumber, higher), Seek.Next.Read)

          case LevelSeek.None =>
            Higher(key, readState, Seek.Current.Stop, nextSeek)
        }

      /** ********************************************************
       * ******************                   *******************
       * ******************  Current Stop     *******************
       * ******************                   *******************
       * ******************************************************* */
      case Seek.Current.Stop =>
        nextSeek match {
          case Seek.Next.Read =>
            nextWalker.higher(key, readState) match {
              case next: KeyValue.Put =>
                Higher(key, readState, currentSeek, Seek.Next.Stash(next))

              case KeyValue.Put.Null =>
                Higher(key, readState, currentSeek, Seek.Next.Stop)
            }

          case Seek.Next.Stash(next) =>
            if (next.hasTimeLeft())
              next
            else
              Higher(next.key, readState, currentSeek, Seek.Next.Read)

          case Seek.Next.Stop =>
            KeyValue.Put.Null
        }

      /** *********************************************************
       * ******************                     *******************
       * ******************    Current.Stash    *******************
       * ******************                     *******************
       * ******************************************************** */

      case currentStash @ Seek.Current.Stash(segmentNumber, current) =>
        nextSeek match {
          case Seek.Next.Read =>
            //decide if it's necessary to read the next Level or not.
            current match {
              //10->19  (input keys)
              //10 - 20 (higher range from current Level)
              case currentRange: KeyValue.Range if key >= currentRange.fromKey =>
                //if the current range is active fetch the highest from next Level and return highest from both Levels.
                if (Value.hasTimeLeft(currentRange.fetchRangeValueUnsafe)) //if the higher from the current Level is a Fixed key-value, fetch from next Level and return the highest.
                  nextWalker.higher(key, readState) match {
                    case next: KeyValue.Put =>
                      Higher(key, readState, currentStash, Seek.Next.Stash(next))

                    case KeyValue.Put.Null =>
                      Higher(key, readState, currentStash, Seek.Next.Stop)
                  }
                else //if the rangeValue is expired then the higher is toKey or ceiling of toKey
                  currentWalker.get(currentRange.toKey, readState) match {
                    case ceiling: KeyValue.Put if ceiling.hasTimeLeft() =>
                      ceiling

                    case _: KeyValue.Put | KeyValue.Put.Null =>
                      Higher(currentRange.toKey, readState, Seek.Current.Read(segmentNumber), nextSeek)
                  }

              //if the input key is smaller than this Level's higher Range's fromKey.
              //0           (input key)
              //    10 - 20 (higher range)
              case _: KeyValue =>
                nextWalker.higher(key, readState) match {
                  case next: KeyValue.Put =>
                    Higher(key, readState, currentStash, Seek.Next.Stash(next))

                  case KeyValue.Put.Null =>
                    Higher(key, readState, currentStash, Seek.Next.Stop)
                }
            }

          case Seek.Next.Stop =>
            current match {
              case current: KeyValue.Put =>
                if (current.hasTimeLeft())
                  current
                else
                  Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Remove =>
                Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Update =>
                Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.Function =>
                Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case _: KeyValue.PendingApply =>
                Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextSeek)

              case current: KeyValue.Range =>
                higherFromValue(key, current.fromKey, current.fetchFromValueUnsafe) match {
                  case somePut: KeyValue.Put =>
                    somePut

                  case KeyValue.Put.Null =>
                    currentWalker.get(current.toKey, readState) match {
                      case put: KeyValue.Put =>
                        if (put.hasTimeLeft())
                          put
                        else
                          Higher(current.toKey, readState, Seek.Current.Read(segmentNumber), nextSeek)

                      case KeyValue.Put.Null =>
                        Higher(current.toKey, readState, Seek.Current.Read(segmentNumber), nextSeek)
                    }
                }
            }

          case nextStash @ Seek.Next.Stash(next) =>
            current match {

              /** **********************************************
               * ******************         *******************
               * ******************  Fixed  *******************
               * ******************         *******************
               * ********************************************* */

              case current: KeyValue.Fixed =>
                //    2
                //    2
                if (next.key equiv current.key)
                  FixedMerger(current, next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _ =>
                      //if it doesn't result in an unexpired put move forward.
                      Higher(current.key, readState, Seek.Current.Read(segmentNumber), Seek.Next.Read)
                  }
                //    2
                //      3  or  5
                else if (next.key > current.key)
                  current match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    //if it doesn't result in an unexpired put move forward.
                    case _ =>
                      Higher(current.key, readState, Seek.Current.Read(segmentNumber), nextStash)
                  }
                //    2
                //0
                else //else higher from next is smaller
                  next

              /** *********************************************
               * *********************************************
               * ******************       ********************
               * ****************** RANGE ********************
               * ******************       ********************
               * *********************************************
               * ******************************************** */
              case current: KeyValue.Range =>
                //   10 - 20
                //1
                if (next.key < current.fromKey)
                  next
                //10 - 20
                //10
                else if (next.key equiv current.fromKey)
                  FixedMerger(current.fetchFromOrElseRangeValueUnsafe.toMemory(current.fromKey), next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _ =>
                      //do need to check if range is expired because if it was then
                      //next would not have been read from next level in the first place.
                      Higher(next.key, readState, currentStash, Seek.Next.Read)
                  }

                //10  -  20
                //  11-19
                else if (next.key < current.toKey) { //if the higher in next Level falls within the range.
                  val (fromValue, rangeValue) = current.fetchFromAndRangeValueUnsafe
                  //if fromValue is set check if it qualifies as the next highest orElse return higher of fromKey
                  higherFromValue(key, current.fromKey, fromValue) match {
                    case fromValuePut: KeyValue.Put =>
                      fromValuePut

                    case KeyValue.Put.Null =>
                      FixedMerger(rangeValue.toMemory(next.key), next) match {
                        case put: KeyValue.Put if put.hasTimeLeft() =>
                          put

                        case _ =>
                          //fetch the next key keeping the current stash. next.key's higher is still current range
                          //since it's < range's toKey
                          Higher(next.key, readState, currentStash, Seek.Next.Read)
                      }
                  }
                }
                //10 - 20
                //     20 ----to----> ∞
                else //else if the higher in next Level does not fall within the range.
                  higherFromValue(key, current.fromKey, current.fetchFromValueUnsafe) match {
                    case put: KeyValue.Put =>
                      put

                    case KeyValue.Put.Null =>
                      currentWalker.get(current.toKey, readState) match {
                        case put: KeyValue.Put =>
                          if (put.hasTimeLeft())
                            put
                          else
                            Higher(current.toKey, readState, Seek.Current.Read(segmentNumber), nextStash)

                        case KeyValue.Put.Null =>
                          Higher(current.toKey, readState, Seek.Current.Read(segmentNumber), nextStash)
                      }
                  }
            }
        }
    }
  }
}
