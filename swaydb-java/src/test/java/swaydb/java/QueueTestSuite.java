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
