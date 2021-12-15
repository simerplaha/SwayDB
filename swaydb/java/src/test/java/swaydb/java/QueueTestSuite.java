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
import swaydb.java.eventually.persistent.EventuallyPersistentQueue;
import swaydb.java.memory.MemoryQueue;
import swaydb.java.persistent.PersistentQueue;
import swaydb.java.serializers.Serializer;

import java.io.IOException;

class MemoryQueueTest extends QueueTest {

  public <K> Queue<K> createQueue(Serializer<K> serialiser) {
    return
      MemoryQueue
        .config(serialiser)
        .get();
  }
}

class PersistentQueueTest extends QueueTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Queue<K> createQueue(Serializer<K> serialiser) throws IOException {
    return
      PersistentQueue
        .config(testDir(), serialiser)
        .get();
  }
}

class EventuallyPersistentQueueTest extends QueueTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Queue<K> createQueue(Serializer<K> serialiser) throws IOException {
    return
      EventuallyPersistentQueue
        .config(testDir(), serialiser)
        .get();
  }
}
