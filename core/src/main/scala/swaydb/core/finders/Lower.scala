/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import swaydb.core.data.{KeyValue, Memory, Persistent, Value}
import swaydb.core.util.MinMax
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Lower {

  def apply(key: Slice[Byte],
            lowerInCurrentLevelOnly: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            lowerInNextLevel: Slice[Byte] => Try[Option[KeyValue.FindResponse]])(implicit ordering: Ordering[Slice[Byte]],
                                                                                 keyValueOrdering: Ordering[KeyValue]): Try[Option[KeyValue.FindResponse]] = {

    import ordering._
    //    println(s"${rootPath}: Lower for key: " + key.readInt())
    @tailrec
    def lower(key: Slice[Byte]): Try[Option[KeyValue.FindResponse]] =
      lowerInCurrentLevelOnly(key) match {
        case Success(lowerFromThis) =>
          lowerFromThis match {
            case Some(lowerFromThisFixed: KeyValue.ReadOnly.Fixed) => //if lower from this Level is a fixed key-value, return max of this and next Level's lower.
              lowerInNextLevel(key) match {
                case Success(lowerFromNext) =>
                  MinMax.max(Some(lowerFromThisFixed), lowerFromNext) match {
                    case Some(lowerRemove: Persistent.Remove) =>
                      lower(lowerRemove.key)

                    case Some(lowerRemove: Memory.Remove) =>
                      lower(lowerRemove.key)

                    case Some(lower: Persistent.Put) =>
                      Success(Some(lower))

                    case Some(lower: Memory.Put) =>
                      Success(Some(lower))

                    case None =>
                      Success(None)
                  }
                case failed @ Failure(_) =>
                  failed
              }

            //key is within the range
            //example
            //  15    (input key)
            //10 - 20 (lower range)
            case Some(lowerFromThisLevelRange: KeyValue.ReadOnly.Range) if key > lowerFromThisLevelRange.fromKey && key <= lowerFromThisLevelRange.toKey =>
              lowerInNextLevel(key) match {

                //example
                //  15    (input key)
                //10      (lower from next Level)
                //10 - 20 (lower from this Level range)
                case Success(Some(lowerInNext)) if lowerInNext.key equiv lowerFromThisLevelRange.fromKey => //if lower from next matches this range's fromKey
                  lowerFromThisLevelRange.fetchFromValue match {
                    case Success(Some(Value.Remove)) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Success(Some(Value.Put(fromValue))) =>
                      Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                    case Success(None) =>
                      lowerFromThisLevelRange.fetchRangeValue match {
                        case Success(Value.Remove) =>
                          lower(lowerFromThisLevelRange.fromKey)

                        case Success(Value.Put(fromValue)) =>
                          Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                        case Failure(exception) =>
                          Failure(exception)
                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }
                //example
                //    15     (input key)
                //  12       (lower from next Level)
                //10  -   20 (lower from this Level range)
                case Success(Some(lowerInNext)) if lowerInNext.key > lowerFromThisLevelRange.fromKey =>
                  lowerFromThisLevelRange.fetchRangeValue match {
                    case Success(Value.Remove) => //if the range is remove.
                      lowerFromThisLevelRange.fetchFromValue match { //check if fromValue is set and return it.
                        case Success(Some(Value.Remove)) => //fromValue is not set, get the lower of fromKey
                          lower(lowerFromThisLevelRange.fromKey)

                        case Success(Some(Value.Put(fromValue))) => //fromValue is set, return this as lower.
                          Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                        case Success(None) => //fromValue is not set, get lower of fromKey
                          lower(lowerFromThisLevelRange.fromKey)

                        case Failure(exception) =>
                          Failure(exception)
                      }

                    case Success(Value.Put(fromValue)) => //range value is put.
                      Success(Some(Memory.Put(lowerInNext.key, fromValue)))

                    case Failure(exception) =>
                      Failure(exception)
                  }

                //example
                //      15      (input key)
                // 9            (lower from next Level or is None)
                //   10  -   20 (lower from this Level range)
                case Success(_) =>
                  lowerFromThisLevelRange.fetchFromValue match {
                    case Success(Some(Value.Remove)) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Success(Some(Value.Put(fromValue))) =>
                      Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                    case Success(None) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //example
            //             25 (input key)
            //10  -   20      (lower from this Level range)
            case Some(lowerFromThisLevelRange: KeyValue.ReadOnly.Range) =>
              lowerInNextLevel(key) match {
                case Success(Some(lowerInNext)) if lowerInNext.key >= lowerFromThisLevelRange.toKey => //lower from next level is right edge: 20 - 24
                  Success(Some(lowerInNext))

                case Success(Some(lowerInNext)) if lowerInNext.key < lowerFromThisLevelRange.fromKey => //lower from next level is left edge: 0 - 9
                  lowerFromThisLevelRange.fetchFromValue match {
                    case Success(Some(Value.Remove)) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Success(Some(Value.Put(fromValue))) =>
                      Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                    case Success(None) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Success(Some(lowerInNext)) if lowerInNext.key equiv lowerFromThisLevelRange.fromKey => //lower from next level is within range: 10
                  lowerFromThisLevelRange.fetchFromValue match {
                    case Success(Some(Value.Remove)) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Success(Some(Value.Put(fromValue))) =>
                      Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                    case Success(None) =>
                      lowerFromThisLevelRange.fetchRangeValue match {
                        case Success(Value.Remove) =>
                          lower(lowerFromThisLevelRange.fromKey)

                        case Success(Value.Put(rangeValue)) =>
                          Success(Some(Memory.Put(lowerInNext.key, rangeValue)))

                        case Failure(exception) =>
                          Failure(exception)
                      }

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Success(Some(lowerInNext)) => //lower from next level is within range: 11 - 19
                  lowerFromThisLevelRange.fetchRangeValue match {
                    case Success(Value.Remove) =>
                      lowerFromThisLevelRange.fetchFromValue match {
                        case Success(Some(Value.Remove)) =>
                          lower(lowerFromThisLevelRange.fromKey)

                        case Success(Some(Value.Put(fromValue))) =>
                          Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                        case Success(None) =>
                          lower(lowerFromThisLevelRange.fromKey)

                        case Failure(exception) =>
                          Failure(exception)
                      }

                    case Success(Value.Put(fromValue)) =>
                      Success(Some(Memory.Put(lowerInNext.key, fromValue)))

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Success(None) => //lower level has no lower, fetch fromValue or fromValue's lower.
                  lowerFromThisLevelRange.fetchFromValue match {
                    case Success(Some(Value.Remove)) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Success(Some(Value.Put(fromValue))) =>
                      Success(Some(Memory.Put(lowerFromThisLevelRange.fromKey, fromValue)))

                    case Success(None) =>
                      lower(lowerFromThisLevelRange.fromKey)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            case None =>
              lowerInNextLevel(key)
          }

        case Failure(exception) =>
          Failure(exception)
      }

    lower(key)
  }

}
