package net.lshift.diffa.versioning;

import org.junit.Test;
import static org.junit.Assert.*;

public class MerkleNodeTest {

  @Test
  public void shouldBuildHierarchyFromString() {

    MerkleNode hydrated = MerkleNode.buildHierarchy("ab.cd.ef");

    MerkleNode leaf = new MerkleNode("ef", null, null);
    MerkleNode middle = new MerkleNode("cd", leaf);
    MerkleNode top = new MerkleNode("ab", middle);
    MerkleNode expected = new MerkleNode("", top);

    assertEquals(expected, hydrated);
  }
}
