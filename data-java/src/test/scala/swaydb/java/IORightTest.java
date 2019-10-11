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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test success conditions for IO.
 */
class IORightTest {

  IO<Throwable, Integer> io = IO.run(() -> 1);

  @Test
  void isRight() throws Throwable {
    assertTrue(io.isRight());
    assertEquals(1, io.get());
    assertFalse(io.isLeft());
  }

  @Test
  void left() {
    assertThrows(UnsupportedOperationException.class, () -> io.leftValue().get());
  }

  @Test
  void right() throws Throwable {
    assertEquals(1, io.rightValue().get());
  }

  @Test
  void map() throws Throwable {
    IO<Throwable, Integer> map = io.map(integer -> integer + 1);
    assertTrue(map.isRight());
    assertEquals(map.get(), 2);
  }

  @Test
  void flatMap() throws Throwable {
    IO<Throwable, Integer> map = io.flatMap(integer -> IO.run(() -> integer + 2));
    assertTrue(map.isRight());
    assertEquals(map.get(), 3);
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

    assertEquals(1, result.get());
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
    assertEquals(exists.get(), 1);

    IO<Throwable, Integer> existsNot = io.filter(integer -> integer == 2);
    assertTrue(existsNot.isLeft());
    assertThrows(NoSuchElementException.class, () -> existsNot.get());
  }

  @Test
  void recoverWith() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recoverWith(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 1);
  }

  @Test
  void recover() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.recover(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 1);
  }

  @Test
  void onLeftSideEffect() throws Throwable {
    IO<Throwable, Integer> recovered =
      io.onLeftSideEffect(throwable -> fail("Unexpected"));

    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 1);
  }

  @Test
  void onRightSideEffect() throws Throwable {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onRightSideEffect(throwable -> executed.set(true));

    assertTrue(executed.get());
    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 1);
  }

  @Test
  void onCompleteSideEffect() throws Throwable {
    AtomicBoolean executed = new AtomicBoolean(false);
    IO<Throwable, Integer> recovered =
      io.onCompleteSideEffect(throwable -> executed.set(true));

    assertTrue(executed.get());
    assertTrue(recovered.isRight());
    assertEquals(recovered.get(), 1);
  }

  @Test
  void toOptional() {
    Optional<Integer> integer = io.toOptional();
    assertTrue(integer.isPresent());
    assertEquals(1, integer.get());
  }
}
