package net.lshift.diffa.snowflake;

public class SequenceExhaustedException extends Exception {
	private static final long serialVersionUID = -2301105407104624814L;

	public SequenceExhaustedException(int seqNum) {
		super(String.format("Sequence upper bound reached at %d", seqNum));
	}
}
