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
import swaydb.Exception;
import swaydb.config.DataType;
import swaydb.java.persistent.PersistentMap;
import swaydb.java.persistent.PersistentSet;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static swaydb.java.JavaTest.shouldBe;
import static swaydb.java.serializers.Default.intSerializer;


public class PersistentReportRebootErrorsTest extends TestBase {

  @Test
  void reportInvalidDataTypes() throws IOException {
    Path path = testDir().resolve("overlapping_data_types");

    Map<Integer, Integer, Void> map =
      PersistentMap.functionsOff(path, intSerializer(), intSerializer())
        .get();

    map.close();

    Exception.InvalidDirectoryType exception = assertThrows(Exception.InvalidDirectoryType.class, () -> PersistentSet.functionsOff(path, intSerializer()).get());
    shouldBe(exception.invalidType(), DataType.set());
    shouldBe(exception.expected(), DataType.map());
  }

  @Test
  void reportOverlappingLocks() throws IOException {
    Path path = testDir().resolve("overlapping_locks");

    PersistentMap.functionsOff(path, intSerializer(), intSerializer()).get();

    assertThrows(OverlappingFileLockException.class, () -> PersistentMap.functionsOff(path, intSerializer(), intSerializer()).get());
  }
}
