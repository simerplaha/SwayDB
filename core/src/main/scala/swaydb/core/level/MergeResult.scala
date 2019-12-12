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

package swaydb.core.level

object MergeResult {

  val `false` = MergeResult(false)
  val `true` = MergeResult(true)

  def apply[T](value: T): MergeResult[T] =
    new MergeResult[T](
      levelsUpdated = Set.empty,
      value = value
    )

  def apply[T](value: T,
               levelsUpdated: Set[Int]): MergeResult[T] =
    new MergeResult[T](
      value = value,
      levelsUpdated = levelsUpdated
    )

  def apply[T](value: T,
               levelUpdated: Int): MergeResult[T] =
    new MergeResult[T](
      value = value,
      levelsUpdated = Set(levelUpdated)
    )
}

/**
 * Maintains the state of all levels that were updated during the merge and the merge outcome.
 */
class MergeResult[T](val value: T,
                     val levelsUpdated: Set[Int]) {
  def transform[B](f: T => B): MergeResult[B] =
    MergeResult(
      value = f(value),
      levelsUpdated = levelsUpdated
    )

  def updateValue[B](value: B): MergeResult[B] =
    MergeResult(
      value = value,
      levelsUpdated = levelsUpdated
    )
}
