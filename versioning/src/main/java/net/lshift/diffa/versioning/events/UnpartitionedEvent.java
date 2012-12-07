package net.lshift.diffa.versioning.events;

import net.lshift.diffa.versioning.partitioning.MerkleNode;
import org.joda.time.DateTime;

public interface UnpartitionedEvent extends ChangeEvent {

  String getVersion();

  DateTime getLastUpdated();

  MerkleNode getIdHierarchy();
}
