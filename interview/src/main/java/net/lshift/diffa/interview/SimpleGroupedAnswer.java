package net.lshift.diffa.interview;

public class SimpleGroupedAnswer implements GroupedAnswer {

  private final SerializableGroupedAnswer answer;

  public SimpleGroupedAnswer(SerializableGroupedAnswer answer) {
    this.answer = answer;
  }

  public SimpleGroupedAnswer(String group, String digest) {
    this.answer = SerializableGroupedAnswer.newBuilder().setDigest(digest).setGroup(group).build();
  }

  @Override
  public String getGroup() {
    return answer.getGroup();
  }

  @Override
  public String getDigest() {
    return answer.getDigest();
  }
}
