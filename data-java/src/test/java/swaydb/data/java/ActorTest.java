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

package swaydb.data.java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import swaydb.java.Actor;

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
