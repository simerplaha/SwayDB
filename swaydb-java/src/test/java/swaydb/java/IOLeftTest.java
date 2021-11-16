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
import swaydb.java.IO;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test success conditions for IO.
 */
class IOLeftTest {

  private static class FailedIO extends RuntimeException {
  }

  IO<Throwable, Integer> io =
    IO.run(
      () -> {
        throw new FailedIO();
      }
    );

  @Test
  void isLeft() {
    assertFalse(io.isRight());
    assertThrows(FailedIO.class, () -> io.tryGet());
    assertTrue(io.isLeft());
  }

  @Test
  void leftIO() {
    assertDoesNotThrow(() -> io.leftIO().tryGet());
  }

  @Test
  void rightIO() {
    assertThrows(UnsupportedOperationException.class, () -> io.rightIO().tryGet());
  }

  @Test
  void map() {
    IO<Throwable, Integer> map = io.map(integer -> integer + 1);
    assertTrue(map.isLeft());
  }

  @Test
  void flatMap() {
    IO<Throwable, Integer> map = io.flatMap(integer -> IO.run(() -> integer + 2));
    assertTrue(map.isLeft());
  }

  @Test
  void orElseGet() {
    Integer result = io.orElseGet(() -> 22);
    assertEquals(22, result);
  }

  @Test
  void or() throws Throwable {
    IO<Throwable, Integer> result =
      io.or(() -> IO.right(222));

    assertEquals(222, result.tryGet());
  }

  @Test
  void forEach() {
    AtomicBoolean executed = new AtomicBoolean(false);
    io.forEach(integer -> executed.set(true));
    assertFalse(executed.get());
  }

  @Test
  void exists() {
    assertFalse(io.exists(integer -> integer == 1));
    assertFalse(io.exists(integer -> integer == 2));
  }


  @Test
  void filter() {
    IO<Throwable, Integer> existsNot = io.filter(integer -> integer == 12);
    assertTrue(existsNot.isLeft());
    assertThrows(FailedIO.class, () -> existsNot.tryGet());
  }

  @Test
  void recoverWith() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recoverWith(throwable -> IO.right(22222));

    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 22222);
  }

  @Test
  void recover() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recover(throwable -> 22222);

    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 22222);
  }

  @Test
  void onLeftSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onLeftSideEffect(throwable -> executed.set(true));

    assertEquals(io, recovered);

    assertTrue(executed.get());
  }

  @Test
  void onRightSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    io.onRightSideEffect(throwable -> executed.set(true));

    assertFalse(executed.get());
  }

  @Test
  void onCompleteSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onCompleteSideEffect(throwable -> executed.set(true));

    assertTrue(executed.get());
    assertTrue(recovered.isLeft());
  }

  @Test
  void toOptional() {
    Optional<Integer> integer = io.toOptional();
    assertFalse(integer.isPresent());
  }
}
