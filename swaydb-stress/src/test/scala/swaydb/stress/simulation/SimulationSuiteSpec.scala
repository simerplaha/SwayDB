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

package swaydb.stress.simulation

import swaydb.IO
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.MMAP
import swaydb.serializers.Default._
import swaydb.stress.simulation.Domain._

class Memory_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: swaydb.Map.Functions[Long, Domain, Functions],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.Map[Long, Domain, Functions, IO.ApiIO]().get.sweep()
}

class Persistent_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: swaydb.Map.Functions[Long, Domain, Functions],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Long, Domain, Functions, IO.ApiIO](
      dir = randomDir,
      mmapMaps = MMAP.randomForMap(),
      mmapAppendix = MMAP.randomForMap(),
      cacheKeyValueIds = randomBoolean(),
      acceleration = Accelerator.brake(),
      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep()
}

class Memory_Persistent_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: swaydb.Map.Functions[Long, Domain, Functions],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Long, Domain, Functions, IO.ApiIO](
      dir = randomDir,
      mmapMaps = MMAP.randomForMap(),
      mmapAppendix = MMAP.randomForMap(),
      cacheKeyValueIds = randomBoolean(),
      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep()
}
