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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.multimap

import swaydb.{MultiMap_Experimental, Prepare}

import scala.concurrent.duration.Deadline

/**
 * Holds [[Prepare]] statements which than get converted to [[MultiMap_Experimental.innerMap]]'s [[Prepare]] type.
 *
 * @param thisMapKey        [[MultiMap_Experimental]] key's
 * @param defaultExpiration [[MultiMap_Experimental]] default expiration
 * @param prepare           The [[Prepare]] statemented created for the [[MultiMap_Experimental]]
 */
class Transaction[+M, +K, +V, +F](val thisMapKey: Iterable[M],
                                  val defaultExpiration: Option[Deadline],
                                  val prepare: Prepare[K, V, F])