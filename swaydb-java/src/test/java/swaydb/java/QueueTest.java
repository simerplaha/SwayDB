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

import org.junit.jupiter.api.Test;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static swaydb.java.JavaTest.foreachRange;
import static swaydb.java.JavaTest.shouldContain;
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
