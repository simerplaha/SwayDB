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

package swaydb.core.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag
import swaydb.Bag.Implicits._
import swaydb.core.file.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.{ByteBufferSweeper, FileSweeper}
import swaydb.core.segment.cache.sweeper.MemorySweeper

object LevelCloser extends LazyLogging {

  def close[BAG[_]]()(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                      blockCacheSweeper: Option[MemorySweeper.Block],
                      fileSweeper: FileSweeper,
                      bufferCleaner: ByteBufferSweeperActor,
                      bag: Bag[BAG]): BAG[Unit] = {

    MemorySweeper.close(keyValueMemorySweeper)
    MemorySweeper.close(blockCacheSweeper)

    FileSweeper.close()
      .and(ByteBufferSweeper.close())
  }
}
