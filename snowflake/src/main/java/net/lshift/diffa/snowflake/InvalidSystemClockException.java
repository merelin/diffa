package net.lshift.diffa.snowflake;

public class InvalidSystemClockException extends Exception {
	private static final long serialVersionUID = -9187977083903839232L;

	public InvalidSystemClockException() {
		super("System clock ran backwards");
	}
}
