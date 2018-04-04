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

object Higher {

  def apply(key: Slice[Byte],
            higherInCurrentLevelOnly: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            ceiling: Slice[Byte] => Try[Option[KeyValue.FindResponse]],
            higherInNextLevel: Slice[Byte] => Try[Option[KeyValue.FindResponse]])(implicit ordering: Ordering[Slice[Byte]],
                                                                                  keyValueOrdering: Ordering[KeyValue]): Try[Option[KeyValue.FindResponse]] = {

    import ordering._

    //    println(s"${rootPath}: Higher for key: " + key.readInt())

    @tailrec
    def higher(key: Slice[Byte]): Try[Option[KeyValue.FindResponse]] =
      higherInCurrentLevelOnly(key) match {
        case Success(higherFromThis) =>
          higherFromThis match {
            case Some(higherFromThisFixed: KeyValue.ReadOnly.Fixed) => //if higher from this Level is a fixed key-value, return min of this and next Level's higher.
              higherInNextLevel(key) match {
                case Success(higherFromNext) =>
                  MinMax.min(Some(higherFromThisFixed), higherFromNext) match {
                    case Some(higherRemove: Persistent.Remove) =>
                      higher(higherRemove.key)

                    case Some(higherRemove: Memory.Remove) =>
                      higher(higherRemove.key)

                    case Some(higher: Persistent.Put) =>
                      Success(Some(higher))

                    case Some(higher: Memory.Put) =>
                      Success(Some(higher))

                    case None =>
                      Success(None)
                  }
                case failed @ Failure(_) =>
                  failed
              }

            //if the input key falls within the higher range of this Level.
            //example
            //10 (input key)
            //10 - 20 (higher range)
            case Some(higherFromThisRange: KeyValue.ReadOnly.Range) if key >= higherFromThisRange.fromKey =>
              higherFromThisRange.fetchRangeValue match {
                case Success(_: Value.Remove) => //if the rangeValue is remove then the higher is ceilingInCurrentLevel of toKey
                  ceiling(higherFromThisRange.toKey)

                case Success(higherInThisRangeValue: Value.Put) => //if the higher rangeValue is Put range fetch higher from next Level
                  higherInNextLevel(key) match {
                    case Success(Some(higherInNext)) =>
                      if (higherInNext.key < higherFromThisRange.toKey) //if the higher in next Level falls within the range
                        Success(Some(Memory.Put(higherInNext.key, higherInThisRangeValue.value)))
                      else //else next Level's higher key doesn't fall in the Range, get ceilingInCurrentLevel of toKey
                        ceiling(higherFromThisRange.toKey)

                    case Success(None) =>
                      ceiling(higherFromThisRange.toKey)

                    case failed @ Failure(_) =>
                      failed
                  }

                case Failure(exception) =>
                  Failure(exception)
              }

            //if the input key is small than this Level's higher Range's from Key.
            //example:
            //0 (input key)
            //    10 - 20 (higher range)
            case Some(higherFromThisLevelRange: KeyValue.ReadOnly.Range) =>
              higherInNextLevel(key) match {
                case Success(Some(higherInNext)) =>
                  if (higherInNext.key < higherFromThisLevelRange.fromKey) //left edge (1 to 9): if the higher in next Level is smaller than higher in this Level return next Level's higher.
                    Success(Some(higherInNext))
                  else if (higherInNext.key >= higherFromThisLevelRange.toKey) //right edge (20 to ∞): if the higher in next Level is >= higher's toKey in this Level return fromValue else ceilingInCurrentLevel of toKey.
                    higherFromThisLevelRange.fetchFromValue match { //if fromValue is defined and it's Put, return fromValue.
                      case Success(Some(Value.Put(value))) =>
                        Success(Some(Memory.Put(higherFromThisLevelRange.key, value)))

                      case Success(Some(Value.Remove)) | Success(None) => //if fromValue is not defined get ceilingInCurrentLevel of toKey
                        ceiling(higherFromThisLevelRange.toKey)

                      case Failure(exception) =>
                        Failure(exception)
                    }
                  else //middle (10 - 20): else higher from next Level is within the range
                    higherFromThisLevelRange.fetchFromValue match { //if fromValue is defined, return fromValue.
                      case Success(Some(Value.Put(value))) =>
                        Success(Some(Memory.Put(higherFromThisLevelRange.key, value)))

                      case Success(Some(Value.Remove)) if higherInNext.key equiv higherFromThisLevelRange.fromKey => //if fromValue is remove
                        higher(higherFromThisLevelRange.fromKey)

                      case Success(Some(Value.Remove)) | Success(None) => //next Level's key is not this Level range's fromKey.
                        higherFromThisLevelRange.fetchRangeValue match { //fetch Range value
                          case Success(Value.Put(rangeValue)) =>
                            Success(Some(Memory.Put(higherInNext.key, rangeValue)))

                          case Success(Value.Remove) => //if range value is remove get ceilingInCurrentLevel
                            ceiling(higherFromThisLevelRange.toKey)

                          case Failure(exception) =>
                            Failure(exception)
                        }

                      case Failure(exception) =>
                        Failure(exception)
                    }

                case Success(None) =>
                  higherFromThisLevelRange.fetchFromValue match { //if fromValue is defined, return fromValue.
                    case Success(Some(Value.Put(value))) =>
                      Success(Some(Memory.Put(higherFromThisLevelRange.key, value)))

                    case Success(Some(Value.Remove)) | Success(None) => //return ceilingInCurrentLevel of toKey.
                      ceiling(higherFromThisLevelRange.toKey)

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case failed @ Failure(_) =>
                  failed
              }

            case None =>
              higherInNextLevel(key)
          }

        case Failure(exception) =>
          Failure(exception)
      }

    higher(key)
  }

}
