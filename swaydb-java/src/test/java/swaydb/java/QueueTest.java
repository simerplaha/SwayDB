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
import swaydb.java.memory.QueueBuilder;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;

class MemoryQueueTest extends QueueTest {

  public <K> Queue<K> createQueue(Serializer<K> serialiser) {
    return
      QueueBuilder
        .builder(serialiser)
        .build();
  }
}

class PersistentQueueTest extends QueueTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Queue<K> createQueue(Serializer<K> serialiser) throws IOException {
    return
      swaydb.java.persistent.QueueBuilder
        .builder(testDir(), serialiser)
        .build();
  }
}

abstract class QueueTest extends TestBase implements JavaEventually {

  public abstract <K> Queue<K> createQueue(Serializer<K> keySerializer) throws IOException;

  @Test
  void pushTest() throws IOException {
    Queue<Integer> queue = createQueue(intSerializer());

    queue.push(1);
    queue.push(2);

    assertEquals(1, queue.popOrNull());
    assertEquals(2, queue.popOrNull());
    assertNull(queue.popOrNull());
  }

  @Test
  void pushExpireTest() throws IOException, InterruptedException {
    Queue<Integer> queue = createQueue(intSerializer());

    queue.push(1, Duration.ofSeconds(1));
    queue.push(2);

    Thread.sleep(1000);

    assertEquals(2, queue.popOrNull());
    assertNull(queue.popOrNull());
  }

  @Test
  void pushManyTest() throws IOException {
    Queue<Integer> queue = createQueue(intSerializer());

    IntStream
      .range(1, 1000000)
      .forEach(queue::push);

    IntStream
      .range(1, 1000000)
      .forEach(
        integer ->
          assertEquals(integer, queue.popOrNull())
      );

    assertNull(queue.popOrNull());
  }
}
