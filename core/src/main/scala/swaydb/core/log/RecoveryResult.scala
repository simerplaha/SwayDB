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

package swaydb.core.log

import swaydb.IO

/**
 * Files can be partially recovered based on the value set for [[swaydb.config.RecoveryMode]].
 *
 * This instance stores the result of the recovery of the target [[swaydb.core.file.DBFile]]
 * and the result of each partial recovery.
 *
 * This instance will only contain failure if the file was partially recovered. If there was a full failure then
 * a [[IO]] outside this instance should return the failure.
 */
sealed case class RecoveryResult[+T](item: T, result: IO[swaydb.Error.Log, Unit])
