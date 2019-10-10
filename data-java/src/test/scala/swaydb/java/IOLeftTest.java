/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test success conditions for IO.
 */
class IOLeftTest {

  private class FailedIO extends RuntimeException {
  }

  IO<Throwable, Integer> io = IO.run(
    () -> {
      throw new FailedIO();
    }
  );

  @Test
  void isLeft() {
    assertFalse(io.isRight());
    assertThrows(FailedIO.class, () -> io.get());
    assertTrue(io.isLeft());
  }

  @Test
  void left() {
    assertDoesNotThrow(() -> io.leftValue().get());
  }

  @Test
  void right() {
    assertThrows(UnsupportedOperationException.class, () -> io.rightValue().get());
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
      io.or(
        () -> IO.right(222)
      );

    assertEquals(222, result.get());
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
  void filter() throws Throwable {
    IO<Throwable, Integer> existsNot = io.filter(integer -> integer == 12);
    assertTrue(existsNot.isLeft());
    assertThrows(FailedIO.class, () -> existsNot.get());
  }

  @Test
  void recoverWith() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recoverWith(
        throwable -> IO.right(22222)
      );

    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 22222);
  }

  @Test
  void recover() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recover(
        throwable -> 22222
      );

    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 22222);
  }

  @Test
  void onLeftSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onLeftSideEffect(
        throwable -> {
          executed.set(true);
        }
      );

    assertEquals(io, recovered);

    assertTrue(executed.get());
  }

  @Test
  void onRightSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    io.onRightSideEffect(
      throwable -> {
        executed.set(true);
      }
    );

    assertFalse(executed.get());
  }

  @Test
  void onCompleteSideEffect() {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onCompleteSideEffect(
        throwable -> {
          executed.set(true);
        }
      );

    assertTrue(executed.get());
    assertTrue(recovered.isLeft());
  }

  @Test
  void toOptional() {
    Optional<Integer> integer = io.toOptional();
    assertTrue(integer.isEmpty());
  }
}
