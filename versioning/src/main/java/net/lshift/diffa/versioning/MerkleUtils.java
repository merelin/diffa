package net.lshift.diffa.versioning;

public class MerkleUtils {

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
