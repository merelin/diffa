package net.lshift.diffa.snowflake;

public class SystemTimeFunction implements TimeFunction {
	private static final TimeFunction systemClock = new SystemTimeFunction();

	public static TimeFunction getInstance() {
		return systemClock;
	}

	private SystemTimeFunction() {}

	@Override
	public long now() {
		return System.currentTimeMillis();
	}
}
