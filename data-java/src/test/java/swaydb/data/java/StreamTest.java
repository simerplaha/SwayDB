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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.java;

import org.junit.jupiter.api.Test;
import swaydb.Pair;
import swaydb.java.Stream;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamTest {
  List<Integer> source = Arrays.asList(1, 2, 3, 4, 5);

  @Test
  void rangeInt() {
    List<Integer> range = Stream.range(1, 10).materialize();
    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), range);
  }

  @Test
  void rangeChar() {
    List<Character> range = Stream.range('a', 'd').materialize();
    assertEquals(Arrays.asList('a', 'b', 'c', 'd'), range);
  }


  @Test
  void mapIO() {

    Stream<Integer> stream = Stream.of(source.iterator());

    List<Integer> streamIntegers =
      stream
        .map(integer -> integer + 10)
        .materialize();

    assertEquals(Arrays.asList(11, 12, 13, 14, 15), streamIntegers);
  }

  @Test
  void flatMapIO() {

    Stream<Integer> stream = Stream.of(source.iterator());

    List<Integer> streamIntegers =
      stream
        .flatMap(integer -> Stream.of(Collections.singletonList(integer + 20).iterator()))
        .materialize();

    assertEquals(Arrays.asList(21, 22, 23, 24, 25), streamIntegers);
  }

  @Test
  void partition() {

    Stream<Integer> stream = Stream.of(source.iterator());

    Pair<List<Integer>, List<Integer>> streamIntegers =
      stream
        .partition(integer -> integer % 2 == 0);

    assertEquals(Arrays.asList(2, 4), streamIntegers.left());
    assertEquals(Arrays.asList(1, 3, 5), streamIntegers.right());
  }
}
