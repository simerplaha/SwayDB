/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level

object CompactionResult {

  val `false` = CompactionResult(false)
  val `true` = CompactionResult(true)

  def apply[T](value: T): CompactionResult[T] =
    new CompactionResult[T](
      levelsUpdated = Set.empty,
      value = value
    )

  def apply[T](value: T,
               levelsUpdated: Set[Int]): CompactionResult[T] =
    new CompactionResult[T](
      value = value,
      levelsUpdated = levelsUpdated
    )

  def apply[T](value: T,
               levelUpdated: Int): CompactionResult[T] =
    new CompactionResult[T](
      value = value,
      levelsUpdated = Set(levelUpdated)
    )
}

/**
 * Maintains the state of all levels that were updated during the merge and the merge outcome.
 */
class CompactionResult[T](val value: T,
                          val levelsUpdated: Set[Int]) {
  def transform[B](f: T => B): CompactionResult[B] =
    CompactionResult(
      value = f(value),
      levelsUpdated = levelsUpdated
    )

  def updateValue[B](value: B): CompactionResult[B] =
    CompactionResult(
      value = value,
      levelsUpdated = levelsUpdated
    )
}
