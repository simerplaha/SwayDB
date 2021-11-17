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

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test success conditions for IO.
 */
class IORightTest {

  IO<Throwable, Integer> io = IO.run(() -> 1);

  @Test
  void isRight() throws Throwable {
    assertTrue(io.isRight());
    assertEquals(1, io.tryGet());
    assertFalse(io.isLeft());
  }

  @Test
  void left() {
    assertThrows(UnsupportedOperationException.class, () -> io.leftIO().tryGet());
  }

  @Test
  void right() throws Throwable {
    assertEquals(1, io.rightIO().tryGet());
  }

  @Test
  void neverException() {
    IO<Integer, String> right = IO.rightNeverException("Some value");
    assertTrue(right.isRight());
    assertFalse(right.isLeft());
    assertEquals("Some value", right.get());
  }

  @Test
  void map() throws Throwable {
    IO<Throwable, Integer> map = io.map(integer -> integer + 1);
    assertTrue(map.isRight());
    assertEquals(map.tryGet(), 2);
  }

  @Test
  void flatMap() throws Throwable {
    IO<Throwable, Integer> map = io.flatMap(integer -> IO.run(() -> integer + 2));
    assertTrue(map.isRight());
    assertEquals(map.tryGet(), 3);
  }

  @Test
  void orElseGet() {
    Integer result =
      io.orElseGet(() -> fail("Should not have executed this"));

    assertEquals(1, result);
  }

  @Test
  void or() throws Throwable {
    IO<Throwable, Integer> result =
      io.or(() -> fail("Should not have executed this"));

    assertEquals(1, result.tryGet());
  }

  @Test
  void forEach() {
    AtomicBoolean executed = new AtomicBoolean(false);
    io.forEach(integer -> executed.set(true));
    assertTrue(executed.get());
  }

  @Test
  void exists() {
    assertTrue(io.exists(integer -> integer == 1));
    assertFalse(io.exists(integer -> integer == 2));
  }


  @Test
  void filter() throws Throwable {
    IO<Throwable, Integer> exists = io.filter(integer -> integer == 1);
    assertTrue(exists.isRight());
    assertEquals(exists.tryGet(), 1);

    IO<Throwable, Integer> existsNot = io.filter(integer -> integer == 2);
    assertTrue(existsNot.isLeft());
    assertThrows(NoSuchElementException.class, () -> existsNot.tryGet());
  }

  @Test
  void recoverWith() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recoverWith(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 1);
  }

  @Test
  void recover() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recover(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 1);
  }

  @Test
  void onLeftSideEffect() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.onLeftSideEffect(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 1);
  }

  @Test
  void onRightSideEffect() throws Throwable {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onRightSideEffect(throwable -> executed.set(true));

    assertTrue(executed.get());
    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 1);
  }

  @Test
  void onCompleteSideEffect() throws Throwable {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onCompleteSideEffect(throwable -> executed.set(true));

    assertTrue(executed.get());
    assertTrue(recovered.isRight());
    assertEquals(recovered.tryGet(), 1);
  }

  @Test
  void toOptional() {
    Optional<Integer> integer = io.toOptional();
    assertTrue(integer.isPresent());
    assertEquals(1, integer.get());
  }
}
