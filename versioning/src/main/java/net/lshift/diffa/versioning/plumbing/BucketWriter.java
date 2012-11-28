package net.lshift.diffa.versioning.plumbing;

import net.lshift.diffa.versioning.MerkleNode;

public interface BucketWriter {

  void write(String bucketName, MerkleNode node);
}
