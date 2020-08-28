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

package swaydb.stress.weather

import swaydb.IO
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.MMAP
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._

class Memory_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.memory.Map[Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class Memory_MultiMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.memory.MultiMap_EAP[Int, Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class Persistent_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](
      dir = randomDir,
      //      mmapMaps = MMAP.randomForMap(),
      //      mmapAppendix = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      acceleration = Accelerator.brake(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment()),
      //      memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb)
    ).get.sweep()
}

class Persistent_MultiMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap_EAP[Int, Int, WeatherData, Nothing, IO.ApiIO](
      dir = randomDir,
      //      mmapMaps = MMAP.randomForMap(),
      //      mmapAppendix = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      acceleration = Accelerator.brake(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment()),
      //      memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb)
    ).get.sweep()
}

class Persistent_SetMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.SetMap[Int, WeatherData, Nothing, IO.ApiIO](
      dir = randomDir,
      //      mmapMaps = MMAP.randomForMap(),
      //      mmapAppendix = MMAP.randomForMap(),
      //      cacheKeyValueIds = randomBoolean(),
      //      acceleration = Accelerator.brake(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment()),
      //      memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb),
    ).get.sweep()
}

class Memory_SetMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.memory.SetMap[Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class EventuallyPersistent_WeatherDataSpec extends WeatherDataSpec {
  //  override def newDB()(implicit sweeper: TestCaseSweeper) = swaydb.eventually.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](randomDir, maxOpenSegments = 10, memoryCacheSize = 10.mb, maxMemoryLevelSize = 500.mb).get
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.eventually.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](
      dir = randomDir,
      //      cacheKeyValueIds = randomBoolean(),
      //      mmapPersistentLevelAppendix = MMAP.randomForMap(),
      //      segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig(randomBoolean()).copyWithMmap(MMAP.randomForSegment())
    ).get.sweep()
}
