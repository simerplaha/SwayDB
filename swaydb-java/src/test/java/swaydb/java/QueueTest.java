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

import org.junit.jupiter.api.Test;
import swaydb.data.java.TestBase;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;

import static swaydb.data.java.JavaTest.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static swaydb.java.serializers.Default.intSerializer;

abstract class QueueTest extends TestBase {

  public abstract <K> Queue<K> createQueue(Serializer<K> keySerializer) throws IOException;

  @Test
  void pushTest() throws IOException {
    Queue<Integer> queue = createQueue(intSerializer());

    queue.push(1);
    queue.push(2);

    assertEquals(1, queue.popOrNull());
    assertEquals(2, queue.popOrNull());
    assertNull(queue.popOrNull());

    queue.delete();
  }

  @Test
  void pushExpireTest() throws IOException, InterruptedException {
    Queue<Integer> queue = createQueue(intSerializer());

    queue.push(1, Duration.ofSeconds(1));
    queue.push(2);

    Thread.sleep(1010);

    assertEquals(2, queue.popOrNull());
    assertNull(queue.popOrNull());

    queue.delete();
  }

  @Test
  void pushManyTest() throws IOException {
    Queue<Integer> queue = createQueue(intSerializer());

    foreachRange(1, 1000000, queue::push);
    foreachRange(1, 1000000, integer -> assertEquals(integer, queue.popOrNull()));

    assertNull(queue.popOrNull());

    queue.delete();
  }

  @Test
  void stream() throws IOException {
    Queue<Integer> queue = createQueue(intSerializer());

    foreachRange(1, 1000, queue::push);
    foreachRange(1, 1000, integer -> shouldContain(queue.pop(), integer));

    queue.delete();
  }
}
