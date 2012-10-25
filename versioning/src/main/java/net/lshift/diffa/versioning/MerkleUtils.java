package net.lshift.diffa.versioning;

public class MerkleUtils {

  public static MerkleNode buildEntityIdNode(String id, String version) {

    if (id == null || id.isEmpty()) {
      throw new RuntimeException("Cannot build a merkle node from an empty string, version was " + version);
    }

    int length = id.length();

    MerkleNode root;
    String rootId = id.substring(0, Math.min(length, 2) );

    if (length > 2) {

      MerkleNode mid;
      String midId = id.substring(2, Math.min(length, 4) );

      if (length > 4) {
        MerkleNode leaf = new MerkleNode(id.substring(4, Math.min(length, 6) ) , id, version);
        mid = new MerkleNode(midId, leaf);
      }
      else {
        mid = new MerkleNode(midId, id, version);
      }

      root = new MerkleNode(rootId, mid);
    }
    else {
      root = new MerkleNode(rootId, id, version);
    }

    return root;
  }
}
