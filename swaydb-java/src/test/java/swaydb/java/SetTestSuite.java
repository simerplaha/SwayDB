/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java;

import org.junit.jupiter.api.AfterEach;
import swaydb.java.eventually.persistent.EventuallyPersistentSet;
import swaydb.java.memory.MemorySet;
import swaydb.java.persistent.PersistentSet;
import swaydb.java.serializers.Serializer;

import java.io.IOException;

class MemorySetTest extends SetTest {

  public <K> Set<K, Void> createSet(Serializer<K> keySerializer) {
    return
      MemorySet
        .functionsOff(keySerializer)
        .get();
  }
}

class PersistentSetTest extends SetTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Set<K, Void> createSet(Serializer<K> keySerializer) throws IOException {
    return
      PersistentSet
        .functionsOff(testDir(), keySerializer)
        .get();
  }
}

class EventuallyPersistentSetTest extends SetTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Set<K, Void> createSet(Serializer<K> keySerializer) throws IOException {
    return
      EventuallyPersistentSet
        .functionsOff(testDir(), keySerializer)
        .get();
  }
}
