package net.lshift.diffa.versioning;


import com.google.common.base.Joiner;
import com.google.common.collect.TreeMultiset;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;
import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.adapter.scanning.MissingAttributeException;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class CassandraVersionStore implements VersionStore {

  static Logger log = LoggerFactory.getLogger(CassandraVersionStore.class);

  static final Joiner KEY_JOINER = Joiner.on(".").skipNulls();
  static final Joiner ATTRIBUTES_JOINER = Joiner.on("_").skipNulls();

  static final String KEY_SPACE = "version_store";

  /**
   * Storage for the raw entity version and their raw partitioning attributes (should they have been supplied)
   */
  static final String ENTITY_VERSIONS_CF = "entity_versions";
  static final String ENTITY_ATTRIBUTES_CF = "entity_attributes";

  /**
   * Storage for Merkle trees for each endpoint using the (optional) user defined partitioning attribute(s)
   */
  static final String USER_DEFINED_BUCKETS_CF = "user_defined_buckets";
  static final String USER_DEFINED_DIGESTS_CF = "user_defined_digests";
  static final String USER_DEFINED_HIERARCHY_CF = "user_defined_hierarchy";
  //static final String USER_DEFINED_HIERARCHY_DIGESTS_CF = "user_defined_hierarchy_digests";

  /**
   * Storage for Merkle trees for each endpoint using the (mandatory) entity id field as a partitioning attribute
   */
  static final String ENTITY_ID_BUCKETS_CF = "entity_id_buckets";
  static final String ENTITY_ID_DIGESTS_CF = "entity_id_digests";
  static final String ENTITY_ID_HIERARCHY_CF = "entity_id_hierarchy";
  //static final String ENTITY_ID_HIERARCHY_DIGESTS_CF = "entity_id_hierarchy_digests";

  static final String BUCKET_KEY = "bucket";
  static final String DIGEST_KEY = "digest";
  static final String VERSION_KEY = "version";
  static final String LAST_UPDATE_KEY = "lastUpdate";

  private Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
  private Keyspace keyspace = HFactory.createKeyspace(KEY_SPACE, cluster);

  private ColumnFamilyTemplate<String, String> entityVersionsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, ENTITY_VERSIONS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  private ColumnFamilyTemplate<String, String> entityIdHierarchyDigestsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, ENTITY_ID_DIGESTS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  private ColumnFamilyTemplate<String, String> userDefinedHierarchyDigestsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, USER_DEFINED_DIGESTS_CF,
          StringSerializer.get(),
          StringSerializer.get());


  public void addEvent(Long space, String endpoint, PartitionedEvent event) {

    String id = buildIdentifier(space, endpoint, event.getId());
    String endpointKey = buildEndpointKey(space, endpoint);

    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createStringColumn(VERSION_KEY, event.getVersion()));

    if (event.getLastUpdated() != null) {
      // TODO use a joda serializer instead of converting to java.util.Date
      Date date = event.getLastUpdated().toDate();
      mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createColumn(LAST_UPDATE_KEY, date, StringSerializer.get(), DateSerializer.get()));
    }

    MerkleNode entityIdRootNode = new MerkleNode("", event.getIdHierarchy());

    recordLineage(endpointKey, entityIdRootNode, mutator, entityIdHierarchyDigestsTemplate, ENTITY_ID_BUCKETS_CF, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);

    Map<String, String> attributes = event.getAttributes();
    if (attributes != null && !attributes.isEmpty()) {

      for(Map.Entry<String,String> entry : attributes.entrySet()) {
        mutator.addInsertion(id, ENTITY_ATTRIBUTES_CF, HFactory.createStringColumn(entry.getKey(), entry.getValue()));
      }

      MerkleNode userDefinedRootNode = new MerkleNode("", event.getAttributeHierarchy());
      recordLineage(endpointKey, userDefinedRootNode, mutator, userDefinedHierarchyDigestsTemplate, USER_DEFINED_BUCKETS_CF, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
    }

    mutator.execute();

  }

  public SortedMap<String,String> getEntityIdDigests(Long space, String endpoint) {
    return getEntityIdDigests(space, endpoint, null);
  }

  public SortedMap<String,String> getEntityIdDigests(Long space, String endpoint, String bucketName) {
    return getGenericDigests(space, endpoint, bucketName, entityIdHierarchyDigestsTemplate, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);
  }

  public SortedMap<String,String> getUserDefinedDigests(Long space, String endpoint) {
    return  getUserDefinedDigests(space, endpoint, null);
  }

  public SortedMap<String,String> getUserDefinedDigests(Long space, String endpoint, String bucketName) {
    return getGenericDigests(space, endpoint, bucketName, userDefinedHierarchyDigestsTemplate, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
  }

  private void recordLineage(String parentName, MerkleNode node, Mutator mutator, ColumnFamilyTemplate<String,String> hierarchyDigests, String bucketCF, String hierarchyCF, String hierarchyDigestCF) {


    String qualifiedBucketName;

    if (node.getName().isEmpty()) {
      qualifiedBucketName = parentName;
    } else {
      qualifiedBucketName = parentName + "." + node.getName();
    }

    if (node.isLeaf()) {
      // This a leaf node so record the bucket value and it's parent

      mutator.addInsertion(qualifiedBucketName, bucketCF, HFactory.createStringColumn(node.getId(), node.getVersion()));

    }
    else {

      MerkleNode child = node.getChild();
      HColumn<String,Boolean> childColumn = HFactory.createColumn(child.getName(), child.isLeaf(), StringSerializer.get(), BooleanSerializer.get());

      ColumnFamilyResult<String,String> result = hierarchyDigests.queryColumns(qualifiedBucketName);

      if (result.hasResults()) {
        String digestLabel = result.getString(child.getName());
        if (digestLabel == null || digestLabel.isEmpty()) {
          // TODO Consider populating the child value with the digest of the child instead of leaving it empty
          mutator.addInsertion(qualifiedBucketName, hierarchyCF, childColumn);
        }
      }
      else {
        // TODO See todo 4 lines back
        mutator.addInsertion(qualifiedBucketName, hierarchyCF, childColumn);
      }

      // In any event, since we are updating a bucket in this lineage, we need to invalidate the digest of each
      // parent node

      mutator.addInsertion(qualifiedBucketName, hierarchyDigestCF, HFactory.createStringColumn("digest",""));

      recordLineage(qualifiedBucketName, node.getChild(), mutator, hierarchyDigests, bucketCF, hierarchyCF, hierarchyDigestCF);
    }

  }

  private SortedMap<String,String> getGenericDigests(Long space, String endpoint, String bucketName, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String hierarchyDigestCF) {
    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    String key;

    if (bucketName == null || bucketName.isEmpty()) {
      key = buildEndpointKey(space,endpoint);
    }
    else {
      key = buildIdentifier(space, endpoint, bucketName);
    }

    SortedMap<String,String> digest = getDigests(key, false, mutator, hierarchyDigests, hierarchyCF, hierarchyDigestCF);

    mutator.execute();

    return digest;
  }



  private SortedMap<String,String> getDigests(String key, boolean isLeaf, Mutator mutator, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String hierarchyDigestCF) {

    SortedMap<String,String> digests = new TreeMap<String,String>();
    ColumnFamilyResult<String, String> result = hierarchyDigests.queryColumns(key);

    if (isLeaf) {

      // Since this is a leaf node, we need to roll up all of the individual entity digests stored in the bucket CF

      String bucketDigest = buildEntityIdDigest(key);
      cacheDigest(key, mutator, hierarchyDigestCF, bucketDigest);
      digests.put(key, bucketDigest);

    }
    else if (result.hasResults()) {

      String cachedDigest = result.getString(DIGEST_KEY);

      if (cachedDigest == null || cachedDigest.isEmpty()) {

        // There is no cached digest for this key, we need to resolve the children and establish their digests

        SliceQuery<String,String,Boolean> hierarchyQuery =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), BooleanSerializer.get());
        hierarchyQuery.setColumnFamily(hierarchyCF);
        hierarchyQuery.setKey(key);

        ColumnSliceIterator<String,String,Boolean> hierarchyIterator = new ColumnSliceIterator<String,String,Boolean>(hierarchyQuery, "", "",false);

        while (hierarchyIterator.hasNext()) {
          HColumn<String, Boolean> hierarchyColumn = hierarchyIterator.next();

          String child = hierarchyColumn.getName();
          boolean leaf = hierarchyColumn.getValue();

          String qualifiedKey = key + "." + child;

          SortedMap<String,String> childDigests = getDigests(qualifiedKey, leaf, mutator, hierarchyDigests, hierarchyCF, hierarchyDigestCF);

          // We need to roll up the subtree digests to produce an over-arching digest for the current bucket

          Digester digester = new Digester();

          for (Map.Entry<String, String> entry : childDigests.entrySet()) {
            digester.addVersion(entry.getValue());
          }

          String bucketDigest = digester.getDigest();
          digests.put(key, bucketDigest);

          // Don't forget to cache this digest for later use

          cacheDigest(key, mutator, hierarchyDigestCF, bucketDigest);
        }
      }
      else {
        digests.put(key, cachedDigest);
      }

    } else {
      throw new BucketNotFoundException(key);
    }

    return digests;
  }

  private void cacheDigest(String key, Mutator mutator, String hierarchyDigestCF, String bucketDigest) {
    mutator.addInsertion(key, hierarchyDigestCF, HFactory.createStringColumn(DIGEST_KEY, bucketDigest));
  }

  private String buildEntityIdDigest(String key) {

    //String key = buildIdentifier(space, endpoint, bucketPrefix);
    Digester digester = new Digester();

    SliceQuery<String,String,String> query =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    query.setColumnFamily(ENTITY_ID_BUCKETS_CF);
    query.setKey(key);

    ColumnSliceIterator<String,String,String> bucketIterator = new ColumnSliceIterator<String,String,String>(query, "", "",false);
    while (bucketIterator.hasNext()) {
      HColumn<String, String> column = bucketIterator.next();
      String version = column.getValue();
      digester.addVersion(version);
    }

    return digester.getDigest();
  }


  public void deleteEvent(Long space, String endpoint, String id) {
    String key = buildIdentifier(space, endpoint, id);

    // Read the attributes back to work what USER_DEFINED_BUCKETS_CF row to update
    ColumnFamilyResult<String,String> result = entityVersionsTemplate.queryColumns(key);
    String bucket = result.getString(BUCKET_KEY);

    // Delete all data associated with this id from all column families
    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    if (bucket == null) {
      log.warn("Could not resolve a bucket for id: " + key);
    }
    else {
      mutator.addDeletion(bucket, USER_DEFINED_BUCKETS_CF, id, StringSerializer.get());
    }

    mutator.addDeletion(key, ENTITY_VERSIONS_CF);
    mutator.addDeletion(key, ENTITY_ATTRIBUTES_CF);

    mutator.execute();
  }




  private String getBucketName(Long space, String endpoint, ChangeEvent event, Iterable<ScanAggregation> aggregations) {
    TreeMultiset<String> buckets = TreeMultiset.create();

    for (ScanAggregation aggregation : aggregations) {
      String attrVal = event.getAttributes().get(aggregation.getAttributeName());
      if (attrVal == null) {
        throw new MissingAttributeException(event.getId(), aggregation.getAttributeName());
      }

      buckets.add(aggregation.bucket(attrVal));
    }

    String bucket = ATTRIBUTES_JOINER.join(buckets);
    return buildIdentifier(space, endpoint, bucket);
  }

  private String buildIdentifier(Long space, String endpoint, String id) {
    return KEY_JOINER.join(space, endpoint, id);
  }

  private String buildEndpointKey(Long space, String endpoint) {
    return KEY_JOINER.join(space, endpoint);
  }


}
