package net.lshift.diffa.snowflake;

import static java.lang.Math.pow;
import static java.lang.Math.round;

public class IdBroker {
  public static final long timestampMask = 0xffffffffffc00000L;
  public static final long machineIdMask = 0x00000000003ff000L;
  public static final long sequenceMask = 0x0000000000000fffL;

  public static final int timestampBits = 41;
  public static final int machineBits = 10;
  public static final int sequenceBits = 12;

  public static final long timestampUpperBound = round(pow(2, timestampBits)) - 1;
  public static final short machineIdUpperBound = (short) (round(pow(2, machineBits)) - 1);
  public static final short sequenceUpperBound = (short) (round(pow(2, sequenceBits)) - 1);

  public static final int timestampLShift = machineBits + sequenceBits;
  public static final int machineLShift = sequenceBits;
  public static final int sequenceLShift = 0;

  int machineId;
  TimeFunction timeFn = SystemTimeFunction.getInstance();
  long pauseMs = 0L;
  short sequenceNum = 0;
  long lastTimestamp = -1L;
  Object mutex = new Object();

  public IdBroker(int machineId) {
    this.machineId = machineId;
  }

  public void setPauseMs(int pause) {
    this.pauseMs = pause;
  }

  public long getId() throws InvalidSystemClockException, SequenceExhaustedException {
    long now = timeFn.now();

    synchronized(mutex) {
      maybePause();

      if (now < lastTimestamp) {
        throw new InvalidSystemClockException();
      } else if (now > lastTimestamp) {
        sequenceNum = 0;
      } else {
        if (sequenceNum < IdBroker.sequenceUpperBound) {
          sequenceNum++;
        } else {
          throw new SequenceExhaustedException(sequenceNum);
        }
      }
    }
    lastTimestamp = now;

    return (now << timestampLShift) | (machineId << machineLShift) | (sequenceNum << sequenceLShift);
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
