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

package swaydb.stress.simulation

import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.data.{Atomic, Functions}
import swaydb.serializers.Default._
import swaydb.stress.simulation.Domain._
import swaydb.{IO, PureFunction}

class Memory_NonAtomic_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](atomic = Atomic.Off).get.sweep(_.delete().get)
}

class Memory_Atomic_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](atomic = Atomic.On).get.sweep(_.delete().get)
}

class Persistent_NonAtomic_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](
      dir = randomDir,
      //      acceleration = Accelerator.brake(),
      atomic = Atomic.Off,
      //      mmapLogs = MMAP.randomForMap(),
      //      mmapAppendixLogs = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      acceleration = Accelerator.brake(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep(_.delete().get)
}

class Persistent_Atomic_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](
      dir = randomDir,
      //      acceleration = Accelerator.brake(),
      atomic = Atomic.On,
      //      mmapLogs = MMAP.randomForMap(),
      //      mmapAppendixLogs = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      acceleration = Accelerator.brake(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep(_.delete().get)
}

class Memory_NonAtomic_Persistent_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.eventually.persistent.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](
      dir = randomDir,
      //      acceleration = Accelerator.brake(),
      atomic = Atomic.Off,
      //      mmapLogs = MMAP.randomForMap(),
      //      mmapAppendixLogs = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep(_.delete().get)
}

class Memory_Atomic_Persistent_SimulationSpec extends SimulationSpec {

  override def newDB()(implicit functions: Functions[PureFunction.Map[Long, Domain]],
                       sweeper: TestCaseSweeper) =
    swaydb.eventually.persistent.Map[Long, Domain, PureFunction.Map[Long, Domain], IO.ApiIO](
      dir = randomDir,
      //      acceleration = Accelerator.brake(),
      atomic = Atomic.On,
      //      mmapLogs = MMAP.randomForMap(),
      //      mmapAppendixLogs = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep(_.delete().get)
}
