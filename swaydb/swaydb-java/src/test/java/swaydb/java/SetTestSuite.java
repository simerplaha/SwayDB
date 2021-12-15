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
