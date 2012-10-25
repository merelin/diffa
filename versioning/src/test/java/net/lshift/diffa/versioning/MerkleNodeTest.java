package net.lshift.diffa.versioning;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(Theories.class)
public class MerkleNodeTest {

  @DataPoint
  public static TestScenario _1_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode node = new MerkleNode("x", "x", version);
    return new TestScenario("x", version, node);
  }

  @DataPoint
  public static TestScenario _2_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode node = new MerkleNode("xy", "xy", version);
    return new TestScenario("xy", version, node);
  }

  @DataPoint
  public static TestScenario _3_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode leaf = new MerkleNode("c", "abc", version);
    MerkleNode root = new MerkleNode("ab", leaf);
    return new TestScenario("abc", version, root);
  }

  @DataPoint
  public static TestScenario _4_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode leaf = new MerkleNode("cd", "abcd", version);
    MerkleNode root = new MerkleNode("ab", leaf);
    return new TestScenario("abcd", version, root);
  }

  @DataPoint
  public static TestScenario _5_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode leaf = new MerkleNode("e", "abcde", version);
    MerkleNode mid = new MerkleNode("cd", leaf);
    MerkleNode root = new MerkleNode("ab", mid);
    return new TestScenario("abcde", version, root);
  }

  @DataPoint
  public static TestScenario _6_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode leaf = new MerkleNode("ef", "abcdef", version);
    MerkleNode mid = new MerkleNode("cd", leaf);
    MerkleNode root = new MerkleNode("ab", mid);
    return new TestScenario("abcdef", version, root);
  }

  @DataPoint
  public static TestScenario _7_char_id() {
    final String version = RandomStringUtils.randomAlphabetic(10);
    MerkleNode leaf = new MerkleNode("ef", "abcdefg", version);
    MerkleNode mid = new MerkleNode("cd", leaf);
    MerkleNode root = new MerkleNode("ab", mid);
    return new TestScenario("abcdefg", version, root);
  }

  @Theory
  public void shouldBuildEntityId(TestScenario scenario) {
    MerkleNode node = MerkleUtils.buildEntityIdNode(scenario.getId(), scenario.getVersion());
    assertEquals(scenario.getExpected(), node);
  }

  @Test
  public void shouldBuildHierarchyFromString() {

    MerkleNode hydrated = MerkleNode.buildHierarchy("ab.cd.ef");

    MerkleNode leaf = new MerkleNode("ef", null, null);
    MerkleNode middle = new MerkleNode("cd", leaf);
    MerkleNode top = new MerkleNode("ab", middle);
    MerkleNode expected = new MerkleNode("", top);

    assertEquals(expected, hydrated);
  }

  private static class TestScenario {
    private String id;
    private String version;
    private MerkleNode expected;

    public TestScenario(String id, String version, MerkleNode node) {
      this.id = id;
      this.version = version;
      this.expected = node;
    }

    public MerkleNode getExpected() {
      return expected;
    }

    public String getId() {
      return id;
    }

    public String getVersion() {
      return version;
    }
  }
}
