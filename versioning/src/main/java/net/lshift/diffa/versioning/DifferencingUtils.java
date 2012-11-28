package net.lshift.diffa.versioning;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;

import java.util.*;

public class DifferencingUtils {

  static final Joiner KEY_JOINER = Joiner.on(".").skipNulls();

  public static Map<String,BucketDigest> convertAggregates(Set<ScanResultEntry> aggregates) {

    // TODO We should probably validate the aggregates against the bucketing and constraints
    // to make sure that no bogus shit is getting sent

    Map<String,BucketDigest> mapped = new HashMap<String, BucketDigest>();

    for (ScanResultEntry entry : aggregates) {

      if (entry.getVersion() == null) {
        throw new RuntimeException("ScanResultEntry did not contain a version: " + entry);
      }

      if (entry.getId() != null && !entry.getId().isEmpty()) {
        throw new RuntimeException("ScanResultEntry should not contain an id for an aggregate: " + entry);
      }

      Map<String,String> attributes = entry.getAttributes();

      if (attributes == null || attributes.isEmpty()) {
        throw new RuntimeException("ScanResultEntry did not contain attributes: " + entry);
      }
      else {

        // TODO This is probably horribly inefficient, but let's profile it first
        SortedMap<String,String> sorted = new TreeMap<String, String>(attributes);
        String partition = KEY_JOINER.join(sorted.values());

        if (mapped.containsKey(partition)) {
          throw new RuntimeException("Duplicate partition (" + partition + ")");
        } else {
          boolean isLeaf = false; // TODO It might be a bad decision to hard code this

          BucketDigest bucket = new BucketDigest(partition, entry.getVersion(), isLeaf);
          mapped.put(partition, bucket);
        }
      }
    }

    return mapped;
  }

}
