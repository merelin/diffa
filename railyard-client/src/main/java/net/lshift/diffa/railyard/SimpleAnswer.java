package net.lshift.diffa.railyard;

public class SimpleAnswer implements Answer {

  private String digest;

  public SimpleAnswer(String digest) {
    this.digest = digest;
  }

  public String getDigest() {

    return digest;
  }

  public void setDigest(String digest) {
    this.digest = digest;
  }
}
