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
 */

package swaydb.java;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import swaydb.data.java.JavaEventually;
import swaydb.data.java.TestBase;
import swaydb.java.memory.QueueConfig;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static swaydb.java.serializers.Default.intSerializer;


class MemoryQueueTest extends QueueTest {

  public <K> Queue<K> createQueue(Serializer<K> serialiser) {
    Queue<K> map =
      QueueConfig
        .withoutFunctions(serialiser)
        .init();

    return map;
  }
}

class PersistentQueueTest extends QueueTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Queue<K> createQueue(Serializer<K> serialiser) throws IOException {
    Queue<K> map =
      swaydb.java.persistent.QueueConfig
        .withoutFunctions(testDir(), serialiser)
        .init();

    return map;
  }
}

abstract class QueueTest extends TestBase implements JavaEventually {

  public abstract <K> Queue<K> createQueue(Serializer<K> keySerializer) throws IOException;

  @Test
  void pushTest() throws IOException {
    Queue<Integer> set = createQueue(intSerializer());

    set.push(1);
    set.push(2);

    assertEquals(1, set.popOrNull());
    assertEquals(2, set.popOrNull());
    assertNull(set.popOrNull());
  }

  @Test
  void pushExpireTest() throws IOException, InterruptedException {
    Queue<Integer> set = createQueue(intSerializer());

    set.push(1, Duration.ofSeconds(1));
    set.push(2);

    Thread.sleep(1000);

    assertEquals(2, set.popOrNull());
    assertNull(set.popOrNull());
  }
}
