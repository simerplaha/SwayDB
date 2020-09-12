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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.java;

import org.junit.jupiter.api.Test;
import swaydb.data.java.TestBase;
import swaydb.java.persistent.PersistentMap;
import swaydb.java.persistent.PersistentSet;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static swaydb.data.java.CommonAssertions.shouldInclude;
import static swaydb.java.serializers.Default.intSerializer;


public class PersistentReportRebootErrorsTest extends TestBase {

  @Test
  void reportInvalidDataTypes() throws IOException {
    Path path = testDir().resolve("overlapping_data_types");

    Map<Integer, Integer, Void> map =
      PersistentMap.functionsOff(path, intSerializer(), intSerializer())
        .get();

    map.close();

    Exception exception = assertThrows(Exception.class, () -> PersistentSet.functionsOff(path, intSerializer()).get());
    shouldInclude(exception.getMessage(), "Invalid type");
  }

  @Test
  void reportOverlappingLocks() throws IOException {
    Path path = testDir().resolve("overlapping_locks");

    PersistentMap.functionsOff(path, intSerializer(), intSerializer()).get();

    assertThrows(OverlappingFileLockException.class, () -> PersistentMap.functionsOff(path, intSerializer(), intSerializer()).get());
  }
}
