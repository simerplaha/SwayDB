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

package swaydb.java

import java.time.Duration
import java.util.Optional

import swaydb.java.Prepare.Set.{Add, ApplyFunction, Remove}

object PrepareForSet {

  def add[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T): Add[T, F] =
    Add[T, F](elem, Optional.empty())

  def add[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, expireAfter: Duration): Add[T, F] =
    Add[T, F](elem, Optional.of(expireAfter))

  def remove[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T): Remove[T, F] =
    Remove[T, F](elem, Optional.empty(), Optional.empty())

  def remove[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T): Remove[T, F] =
    Remove[T, F](fromElem, Optional.of(toElem), Optional.empty())

  def expire[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, after: Duration): Remove[T, F] =
    Remove[T, F](elem, Optional.empty(), Optional.of(after))

  def expire[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T, after: Duration): Remove[T, F] =
    Remove[T, F](fromElem, Optional.of(toElem), Optional.of(after))

  def applyFunction[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, function: F): ApplyFunction[T, F] =
    ApplyFunction[T, F](elem, Optional.empty(), function)

  def applyFunction[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T, function: F): ApplyFunction[T, F] =
    ApplyFunction[T, F](fromElem, Optional.of(toElem), function)

}
