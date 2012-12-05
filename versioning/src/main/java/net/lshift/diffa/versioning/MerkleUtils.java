package net.lshift.diffa.versioning;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class MerkleUtils {

  private final static DateTimeFormatter YEARLY_FORMAT = DateTimeFormat.forPattern("yyyy");
  private final static DateTimeFormatter MONTHLY_FORMAT = DateTimeFormat.forPattern("MM");
  private final static DateTimeFormatter DAILY_FORMAT = DateTimeFormat.forPattern("dd");

  public static MerkleNode buildDateOnlyNode(DateTime date, String id, String version) {
    MerkleNode leaf = new MerkleNode(DAILY_FORMAT.print(date), id, version);
    MerkleNode monthlyBucket = new MerkleNode(MONTHLY_FORMAT.print(date), leaf);
    return new MerkleNode(YEARLY_FORMAT.print(date), monthlyBucket);
  }

  public static MerkleNode buildEntityIdNode(String id, String version) {

    if (id == null || id.isEmpty()) {
      throw new RuntimeException("Cannot build a merkle node from an empty string, version was " + version);
    }

    Digester digester = new Digester();
    digester.addVersion(id);
    String bucket = digester.getDigest();

    String rootId = bucket.substring(0,1);
    MerkleNode root = new MerkleNode(rootId);

    String midId = bucket.substring(1,2);
    MerkleNode mid = new MerkleNode(midId);

    String leafId = bucket.substring(2,3);
    MerkleNode leaf = new MerkleNode(leafId, id, version);

    root.setChild(mid);
    mid.setChild(leaf);

    return root;
  }
}