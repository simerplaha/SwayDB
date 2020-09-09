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

package swaydb.core.map

import swaydb.IO

/**
 * Files can be partially recovered based on the value set for [[swaydb.data.config.RecoveryMode]].
 *
 * This instance stores the result of the recovery of the target [[swaydb.core.io.file.DBFile]]
 * and the result of each partial recovery.
 *
 * This instance will only contain failure if the file was partially recovered. If there was a full failure then
 * a [[IO]] outside this instance should return the failure.
 */
sealed case class RecoveryResult[+T](item: T, result: IO[swaydb.Error.Map, Unit])
