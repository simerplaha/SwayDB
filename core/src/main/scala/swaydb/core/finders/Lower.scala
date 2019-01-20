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

package swaydb.core.finders

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.FixedMerger
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[core] object Lower {

  /**
    * TO-DO - Use trampolining instead to reduce repeated boilerplate code.
    */
  def apply(key: Slice[Byte],
            lowerFromCurrentLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            lowerFromNextLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                   functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._

    //    println(s"${rootPath}: Lower for key: " + key.readInt())
    @tailrec
    def lower(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] = {
      lowerFromCurrentLevel(key) match {
        case Success(current) =>
          current match {
            /** LOWER FIXED FROM CURRENT LEVEL **/
            case Some(current: KeyValue.ReadOnly.Fixed) => //if the lower from the current Level is a Fixed key-value, fetch from next Level and return the lowest.
              lowerFromNextLevel(key) match {
                case Success(next) =>
                  Max(current, next) match {
                    case found @ Success(Some(_)) =>
                      found

                    case Success(None) =>
                      lower(current.key)

                    case Failure(exception) =>
                      Failure(exception)
                  }
                case Failure(exception) =>
                  Failure(exception)
              }

            /** LOWER RANGE FROM CURRENT LEVEL **/
            //example
            //     20 (input key)
            //10 - 20 (lower range from current Level)
            case Some(current: KeyValue.ReadOnly.Range) if key <= current.toKey =>
              current.fetchFromAndRangeValue match {
                case Success((fromValue, rangeValue)) =>
                  if (Value.hasTimeLeft(rangeValue)) //if the current range is active fetch the lowest from next Level and return lowest from both Levels.
                    lowerFromNextLevel(key) match {
                      case Success(next) =>
                        next match {
                          case Some(next) =>
                            //  11 ->   20 (input keys)
                            //10   -    20 (lower range)
                            //  11 -> 19   (lower possible keys from next)
                            if (next.key > current.fromKey) //if the lower in next Level falls within the range
                              FixedMerger(rangeValue.toMemory(next.key), next) match {
                                case Success(current) =>
                                  Max(current, None) match {
                                    case found @ Success(Some(_)) =>
                                      found

                                    case Success(None) =>
                                      lower(next.key)

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                                case Failure(exception) =>
                                  Failure(exception)
                              }

                            //  11 ->   20 (input keys)
                            //10   -    20 (lower range)
                            //10
                            else if (next.key equiv current.fromKey)
                              FixedMerger(fromValue.getOrElse(rangeValue).toMemory(next.key), next) match {
                                case Success(current) =>
                                  Max(current, None) match {
                                    case found @ Success(Some(_)) =>
                                      found

                                    case Success(None) =>
                                      lower(next.key)

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                                case Failure(exception) =>
                                  Failure(exception)
                              }



                            //       11 ->   20 (input keys)
                            //     10   -    20 (lower range)
                            //0->9
                            else
                              fromValue match {
                                case Some(fromValue) =>
                                  Max(fromValue.toMemory(current.fromKey), None) match {
                                    case found @ Success(Some(_)) =>
                                      found

                                    case Success(None) =>
                                      lower(current.fromKey)

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                                case None =>
                                  lower(current.fromKey)
                              }

                          case None =>
                            fromValue match {
                              case Some(fromValue) =>
                                Max(fromValue.toMemory(current.fromKey), None) match {
                                  case found @ Success(Some(_)) =>
                                    found

                                  case Success(None) =>
                                    lower(current.fromKey)

                                  case Failure(exception) =>
                                    Failure(exception)
                                }
                              case None =>
                                lower(current.fromKey)
                            }
                        }
                      case Failure(exception) =>
                        Failure(exception)
                    }

                  else
                    fromValue match {
                      case Some(fromValue) =>
                        if (Value.hasTimeLeft(fromValue))
                          lowerFromNextLevel(key) match {
                            case Success(next) =>
                              next match {
                                case Some(next) =>
                                  //check for equality from lower Level with fromValue only.
                                  if (current.key equiv next.key)
                                    FixedMerger(fromValue.toMemory(next.key), next) match {
                                      case Success(current) =>
                                        Max(current, None) match {
                                          case found @ Success(Some(_)) =>
                                            found

                                          case Success(None) =>
                                            lower(next.key)

                                          case Failure(exception) =>
                                            Failure(exception)
                                        }
                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                  else
                                    Max(fromValue.toMemory(current.fromKey), None) match {
                                      case found @ Success(Some(_)) =>
                                        found

                                      case Success(None) =>
                                        lower(current.fromKey)

                                      case Failure(exception) =>
                                        Failure(exception)
                                    }

                                case None =>
                                  Max(fromValue.toMemory(current.fromKey), None) match {
                                    case found @ Success(Some(_)) =>
                                      found

                                    case Success(None) =>
                                      lower(current.fromKey)

                                    case Failure(exception) =>
                                      Failure(exception)
                                  }
                              }
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        else
                          lower(current.fromKey)

                      case None =>
                        lower(current.fromKey)
                    }

                case Failure(exception) =>
                  Failure(exception)
              }


            //example:
            //             22 (input key)
            //    10 - 20     (lower range)
            case Some(current: KeyValue.ReadOnly.Range) =>
              lowerFromNextLevel(key) match {
                case Success(next) =>
                  next match {
                    case someNext @ Some(next) =>

                      //                     22 (input key)
                      //      10 - 20
                      //           20 or 21
                      if (next.key >= current.toKey)
                        Success(someNext)

                      //                   22 (input key)
                      //      10   -   20
                      //         11->19
                      else if (next.key > current.fromKey)
                        current.fetchRangeValue match {
                          case Success(rangeValue) =>
                            FixedMerger(rangeValue.toMemory(next.key), next) match {
                              case Success(current) =>
                                Max(current, None) match {
                                  case found @ Success(Some(_)) =>
                                    found

                                  case Success(None) =>
                                    lower(next.key)

                                  case Failure(exception) =>
                                    Failure(exception)
                                }
                              case Failure(exception) =>
                                Failure(exception)
                            }

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      //                22 (input key)
                      //      10  -  20
                      //      10
                      else if (next.key equiv current.fromKey) //if the lower in next Level falls within the range.
                        current.fetchFromOrElseRangeValue match {
                          case Success(fromOrElseRangeValue) =>
                            FixedMerger(fromOrElseRangeValue.toMemory(current.fromKey), next) match {
                              case Success(merged) =>
                                Max(merged, None) match {
                                  case found @ Success(Some(_)) =>
                                    found

                                  case Success(None) =>
                                    lower(current.fromKey)

                                  case Failure(exception) =>
                                    Failure(exception)
                                }

                              case Failure(exception) =>
                                Failure(exception)
                            }

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      //               22 (input key)
                      //       10 - 20
                      //0 to 9
                      else
                        current.fetchFromValue match {
                          case Success(fromValue) =>
                            fromValue match {
                              case Some(fromValue) =>
                                //next does not need to supplied here because it's already know that 'next' is larger than fromValue and there is not the lowest.
                                //returnLower will execute lower on fromKey if the the current does not result to a valid lower key-value.
                                Max(fromValue.toMemory(current.fromKey), None) match {
                                  case found @ Success(Some(_)) =>
                                    found

                                  case Success(None) =>
                                    lower(current.fromKey)

                                  case Failure(exception) =>
                                    Failure(exception)
                                }

                              case None =>
                                lower(current.fromKey)
                            }
                          case Failure(exception) =>
                            Failure(exception)
                        }

                    //no lower key-value in the next Level. Return fromValue orElse jump to toKey.
                    case None =>
                      current.fetchFromValue match {
                        case Success(fromValue) =>
                          fromValue match {
                            case Some(fromValue) =>
                              Max(fromValue.toMemory(current.fromKey), None) match {
                                case found @ Success(Some(_)) =>
                                  found

                                case Success(None) =>
                                  lower(current.fromKey)

                                case Failure(exception) =>
                                  Failure(exception)
                              }
                            case None =>
                              lower(current.fromKey)
                          }
                        case Failure(exception) =>
                          Failure(exception)
                      }
                  }
                case Failure(exception) =>
                  Failure(exception)
              }

            case None =>
              lowerFromNextLevel(key)
          }
        case Failure(exception) =>
          Failure(exception)
      }
    }

    lower(key)
  }
}
