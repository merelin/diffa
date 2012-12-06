package net.lshift.diffa.versioning.partitioning;

/**
 * This is a sketch pad class ATM ....
 */
public class OmniPartitionedEvent extends AbstractPartitionedEvent{

  private MerkleNode userDefined;

  public OmniPartitionedEvent(String version, String id) {
    super(version, id);
    //userDefined = MerkleUtils.buildUserDefinedNode();
  }

  @Override
  public MerkleNode getAttributeHierarchy() {
    return userDefined;
  }
}
