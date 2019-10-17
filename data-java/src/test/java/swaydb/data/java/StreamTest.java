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

package swaydb.data.java;

import org.junit.jupiter.api.Test;
import swaydb.java.Stream;
import swaydb.java.StreamIO;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamTest {
  List<Integer> source = Arrays.asList(1, 2, 3, 4, 5);

  @Test
  void mapIO() throws Throwable {

    StreamIO<Integer> stream = Stream.create(source.iterator());

    List<Integer> streamIntegers =
      stream
        .map(integer -> integer + 10)
        .materialize()
        .tryGet();

    assertEquals(Arrays.asList(11, 12, 13, 14, 15), streamIntegers);
  }

  @Test
  void flatMapIO() throws Throwable {

    StreamIO<Integer> stream = Stream.create(source.iterator());

    List<Integer> streamIntegers =
      stream
        .flatMap(integer -> Stream.create(Collections.singletonList(integer + 20).iterator()))
        .materialize()
        .tryGet();

    assertEquals(Arrays.asList(21, 22, 23, 24, 25), streamIntegers);
  }
}
