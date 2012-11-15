package net.lshift.diffa.versioning;

public class BucketDigest {
  private String name;
  private String digest;
  private boolean isLeaf;

  public BucketDigest(String name, String digest, boolean leaf) {
    this.name = name;
    this.digest = digest;
    isLeaf = leaf;
  }

  public String getDigest() {
    return digest;
  }

  public String getName() {
    return name;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BucketDigest that = (BucketDigest) o;

    if (isLeaf != that.isLeaf) return false;
    if (digest != null ? !digest.equals(that.digest) : that.digest != null) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (digest != null ? digest.hashCode() : 0);
    result = 31 * result + (isLeaf ? 1 : 0);
    return result;
  }
}
