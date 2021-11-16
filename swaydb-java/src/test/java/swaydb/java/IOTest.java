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

import javax.naming.NoPermissionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IOTest {

  //a functions that divides 2 number and throws ArithmeticException if right integer is 0.
  Integer divide(Integer left, Integer right) {
    return left / right;
  }

  @Test
  void divideTest() {
    Integer one = 1;
    Integer zero = 0;

    Integer division =
      IO
        .run(() -> divide(one, zero)) //1 divided by 0 will throw ArithmeticException.
        .recover(exception -> zero + 1) //here we recover from that exception and increment zero by 1.
        .map(newZero -> divide(1, newZero)) //perform divide again with incremented zero.
        .get();

    assertEquals(division, 1);
  }

  @Test
  void createLeftThatNeverFails() {
    IO<String, Integer> left = IO.leftNeverException("some left value");

    assertTrue(left.isLeft());
    assertEquals(left.getLeft(), "some left value");
  }

  @Test
  void createRightThatNeverFails() {
    IO<String, Integer> left = IO.rightNeverException(123);

    assertTrue(left.isRight());
    assertEquals(left.getRight(), 123);
  }

  //For the following ioWithCustomExceptionHandler test.
  //lets define our typed errors using enums.
  enum MyError {
    DatabaseConnectionError, //if there is an exception connecting to a remote database.
    FailedDatabaseLogin, //if there is an logging in.
    SomeOtherFailure //some other unknown failure.
  }

  String connectToDatabase() {
    throw new RuntimeException();
  }

  @Test
  void ioWithCustomExceptionHandler() {

    //First lets build our ExceptionHandler that converts RuntimeExceptions to Enums and vice-versa.
    IO.ExceptionHandler<MyError> exceptionHandler =
      new IO.ExceptionHandler<MyError>() {
        @Override
        public Throwable toException(MyError error) {
          switch (error) {
            case DatabaseConnectionError:
              return new RuntimeException("Failed to connect to database");
            case FailedDatabaseLogin:
              return new NoPermissionException("Failed to login to database");
            default:
              return new IllegalStateException("Unexpected value: " + error);
          }
        }

        @Override
        public MyError toError(Throwable exception) {
          if (exception instanceof RuntimeException) {
            return MyError.DatabaseConnectionError;
          } else if (exception instanceof NoPermissionException) {
            return MyError.FailedDatabaseLogin;
          } else {
            return MyError.SomeOtherFailure;
          }
        }
      };

    //here a successful operations results in a successful output.
    IO<MyError, String> success = IO.run(() -> "success", exceptionHandler);
    assertTrue(success.isRight());
    assertEquals("success", success.get());

    //a failed operation.
    IO<MyError, String> failure = IO.run(this::connectToDatabase, exceptionHandler);
    assertTrue(failure.isLeft());
    assertEquals(MyError.DatabaseConnectionError, failure.getLeft());
  }
}
