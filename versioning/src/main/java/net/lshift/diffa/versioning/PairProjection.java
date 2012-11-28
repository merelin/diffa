package net.lshift.diffa.versioning;

public class PairProjection {
  private Long left, right;

  public PairProjection(Long left, Long right) {
    this.left = left;
    this.right = right;
  }

  public Long getLeft() {
    return left;
  }

  public Long getRight() {
    return right;
  }
}
