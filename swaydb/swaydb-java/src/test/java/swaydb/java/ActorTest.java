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
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class ActorTest {

  @Test
  void createActor() throws InterruptedException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    ConcurrentLinkedDeque<String> messages = new ConcurrentLinkedDeque<>();

    BiConsumer<String, Actor.Instance<String, Void>> execution =
      (message, actor) -> {
        assertNull(actor.state());
        messages.add(message);
      };

    Actor.Ref<String, Void> statelessFIFO =
      Actor
        .fifo(execution, executorService)
        .start();

    for (int i = 0; i < 100; i++) {
      statelessFIFO.send("test" + i);
    }

    Thread.sleep(2000);

    ThrowingSupplier<Boolean> supplier =
      () -> {
        for (int i = 0; i < 100; i++) {
          assertTrue(messages.contains("test" + i));
        }
        return true;
      };

    Boolean result = assertTimeout(ofSeconds(2), supplier);

    assertTrue(result);
  }

  @Test
  void recoverFromFailure() throws InterruptedException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    ConcurrentLinkedDeque<String> successMessages = new ConcurrentLinkedDeque<>();

    RuntimeException failedMessageException = new RuntimeException("Failed messages");

    BiConsumer<String, Actor.Instance<String, Void>> execution =
      (message, actor) -> {
        assertNull(actor.state());
        if (message.contains("10")) { //on the 10th message throw Exception.
          throw failedMessageException;
        } else {
          successMessages.add(message);
        }
      };


    ConcurrentLinkedDeque<String> failedMessages = new ConcurrentLinkedDeque<>();

    Actor.Ref<String, Void> statelessFIFO =
      Actor
        .fifo(execution, executorService)
        .recover(
          (message, error, actor) -> {
            failedMessages.add(message); //recover from messages
            assertEquals(error, failedMessageException);
          })
        .start();

    for (int i = 0; i < 100; i++) {
      statelessFIFO.send("test" + i); //send 100 messages
    }

    Thread.sleep(2000);

    //assert that all messages are processed and the failed 10th message is recovered.
    ThrowingSupplier<Boolean> supplier =
      () -> {
        for (int i = 0; i < 100; i++) {
          if (i != 10)
            assertTrue(successMessages.contains("test" + i));
        }
        return true;
      };

    Boolean result = assertTimeout(ofSeconds(5), supplier);

    assertTrue(failedMessages.contains("test10"));

    assertTrue(result);
  }
}
