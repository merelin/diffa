/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.snowflake;

import static java.lang.Math.pow;
import static java.lang.Math.round;

/**
 * Provider of unique, k-ordered identifiers.  Uniqueness of identifiers across
 * nodes can be achieved by having each node provide a unique machine ID.
 */
public class SnowflakeIdProvider implements IdProvider {
  private static final long timestampMask = 0x000001ffffffffffL;
  private static final long machineIdMask = 0x00000000000003ffL;
  private static final long sequenceMask  = 0x0000000000000fffL;
  public static final int machineBits = 10;
  public static final int sequenceBits = 12;

  public static final short sequenceUpperBound = (short) (round(pow(2, sequenceBits)) - 1);

  public static final int timestampLShift = machineBits + sequenceBits;
  public static final int machineLShift = sequenceBits;
  public static final int sequenceLShift = 0;

  private int machineId;
  private long pauseMs = 0L;
  private TimeFunction timeFn = SystemTimeFunction.getInstance();
  private short sequenceNum = 0;
  private long lastTimestamp = -1L;
  private final Object mutex = new Object();

  public SnowflakeIdProvider(int machineId) {
    this.machineId = machineId;
  }

  public void setTimeFn(TimeFunction timeFn) {
    this.timeFn = timeFn;
  }

  public void setPauseMs(int pause) {
    this.pauseMs = pause;
  }

  public long getId() throws InvalidSystemClockException, SequenceExhaustedException {
    long now = timeFn.now();
    long seq;

    synchronized(mutex) {
      maybePause();

      if (now < lastTimestamp) {
        throw new InvalidSystemClockException();
      } else if (now > lastTimestamp) {
        sequenceNum = 0;
      } else {
        if (sequenceNum < SnowflakeIdProvider.sequenceUpperBound) {
          sequenceNum++;
        } else {
          throw new SequenceExhaustedException(sequenceNum);
        }
      }
      seq = sequenceNum;
      lastTimestamp = now;
    }

    long id = ((now & timestampMask) << timestampLShift)
        | ((machineId & machineIdMask) << machineLShift)
        | ((seq & sequenceMask) << sequenceLShift);
    return id;
  }

  private void maybePause() {
    if (pauseMs > 0) {
      try {
        Thread.sleep(pauseMs);
      } catch (InterruptedException ie) {
      }
    }
  }
}
