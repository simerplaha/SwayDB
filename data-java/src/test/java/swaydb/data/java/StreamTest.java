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

package swaydb.data.java;

import org.junit.jupiter.api.Test;
import swaydb.utils.Pair;
import swaydb.java.Stream;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static swaydb.data.java.JavaTest.shouldBe;

class StreamTest {
  List<Integer> source = Arrays.asList(1, 2, 3, 4, 5);

  @Test
  void rangeInt() {
    Iterable<Integer> range = Stream.range(1, 10).materialize();
    shouldBe(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), range);
  }

  @Test
  void rangeChar() {
    Iterable<Character> range = Stream.range('a', 'd').materialize();
    shouldBe(Arrays.asList('a', 'b', 'c', 'd'), range);
  }


  @Test
  void mapIO() {

    Stream<Integer> stream = Stream.of(source.iterator());

    Iterable<Integer> streamIntegers =
      stream
        .map(integer -> integer + 10)
        .materialize();

    shouldBe(Arrays.asList(11, 12, 13, 14, 15), streamIntegers);
  }

  @Test
  void flatMapIO() {

    Stream<Integer> stream = Stream.of(source.iterator());

    Iterable<Integer> streamIntegers =
      stream
        .flatMap(integer -> Stream.of(Collections.singletonList(integer + 20).iterator()))
        .materialize();

    shouldBe(Arrays.asList(21, 22, 23, 24, 25), streamIntegers);
  }

  @Test
  void partition() {

    Stream<Integer> stream = Stream.of(source.iterator());

    Pair<List<Integer>, List<Integer>> streamIntegers =
      stream
        .partitionList(integer -> integer % 2 == 0);

    shouldBe(Arrays.asList(2, 4), streamIntegers.left());
    shouldBe(Arrays.asList(1, 3, 5), streamIntegers.right());
  }
}
