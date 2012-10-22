package net.lshift.diffa.versioning;


import com.google.common.base.Joiner;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraVersionStore implements VersionStore {

  static Logger log = LoggerFactory.getLogger(CassandraVersionStore.class);

  static final Joiner KEY_JOINER = Joiner.on(".").skipNulls();

  static final String KEY_SPACE = "version_store";

  /**
   * Storage for the raw entity version and their raw partitioning attributes (should they have been supplied)
   */
  static final String ENTITY_VERSIONS_CF = "entity_versions";
  static final String USER_DEFINED_ATTRIBUTES_CF = "user_defined_attributes";

  /**
   * Storage for Merkle trees for each endpoint using the (optional) user defined partitioning attribute(s)
   */
  static final String USER_DEFINED_BUCKETS_CF = "user_defined_buckets";
  static final String USER_DEFINED_DIGESTS_CF = "user_defined_digests";
  static final String USER_DEFINED_HIERARCHY_CF = "user_defined_hierarchy";

  /**
   * Storage for Merkle trees for each endpoint using the (mandatory) entity id field as a partitioning attribute
   */
  static final String ENTITY_ID_BUCKETS_CF = "entity_id_buckets";
  static final String ENTITY_ID_DIGESTS_CF = "entity_id_digests";
  static final String ENTITY_ID_HIERARCHY_CF = "entity_id_hierarchy";

  static final String DIGEST_KEY = "digest";
  static final String VERSION_KEY = "version";
  static final String LAST_UPDATE_KEY = "lastUpdate";
  static final String PARTITION_KEY = "partition";

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

  private ColumnFamilyTemplate<String, String> userDefinedAttributesTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, USER_DEFINED_ATTRIBUTES_CF,
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

    String entityIdPartition = event.getIdHierarchy().getDescendencyPath();
    mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createStringColumn(PARTITION_KEY, entityIdPartition));

    MerkleNode entityIdRootNode = new MerkleNode("", event.getIdHierarchy());

    recordLineage(endpointKey, entityIdRootNode, mutator, entityIdHierarchyDigestsTemplate, ENTITY_ID_BUCKETS_CF, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);

    Map<String, String> attributes = event.getAttributes();
    if (attributes != null && !attributes.isEmpty()) {

      for(Map.Entry<String,String> entry : attributes.entrySet()) {
        mutator.addInsertion(id, USER_DEFINED_ATTRIBUTES_CF, HFactory.createStringColumn(entry.getKey(), entry.getValue()));
      }

      String userDefinedPartition = event.getAttributeHierarchy().getDescendencyPath();
      mutator.addInsertion(id, USER_DEFINED_ATTRIBUTES_CF, HFactory.createStringColumn(PARTITION_KEY, userDefinedPartition));

      MerkleNode userDefinedRootNode = new MerkleNode("", event.getAttributeHierarchy());
      recordLineage(endpointKey, userDefinedRootNode, mutator, userDefinedHierarchyDigestsTemplate, USER_DEFINED_BUCKETS_CF, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
    }

    mutator.execute();

  }

  private String deleteDescendencyPath(String parentPath, MerkleNode node, Mutator<String> mutator, String digestCF) {
    String key = qualifyNodeName(parentPath, node);

    mutator.addInsertion(key, digestCF, HFactory.createStringColumn(DIGEST_KEY, ""));

    if (node.isLeaf()) {
      return key;
    }
    else {
      return deleteDescendencyPath(key, node.getChild(), mutator, digestCF);
    }
  }

  public MerkleNode resolveStoredTree(String key, String id, MerkleNode parent, String hierarchyCF) {


    MerkleNode currentNode = null;

    String rangeStart;

    if (parent != null) {
      key = key + "." + parent.getName();

      // "abc123Sdef123Sxyz".replaceAll("^.*?123S","") returns "xyz"
      String regex = "^.*" + parent.getName();

      String trimmed = id.replaceFirst(regex, "");
      rangeStart = trimmed.substring(0,1);
    }
    else {
      rangeStart = id.substring(0,1);
    }

    SliceQuery<String,String,Boolean> hierarchyQuery =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), BooleanSerializer.get());

    hierarchyQuery.setColumnFamily(hierarchyCF);
    hierarchyQuery.setKey(key);
    hierarchyQuery.setRange(rangeStart, "", false, 1);

    QueryResult<ColumnSlice<String,Boolean>> result = hierarchyQuery.execute();

    List<HColumn<String,Boolean>> columns = result.get().getColumns();

    if (columns == null || columns.isEmpty()) {

      throw new EntityNotFoundException(key);

    } else {

      HColumn<String,Boolean> column = columns.get(0);
      String childPartition = column.getName();
      boolean isLeaf = column.getValue();

      if (isLeaf) {

        return new MerkleNode(childPartition, id, null);

      } else {

        currentNode = new MerkleNode(childPartition);

        MerkleNode child = resolveStoredTree(key, id, currentNode, hierarchyCF);
        currentNode.setChild(child);

      }
    }

    return currentNode;
  }



  public void deleteEvent(Long space, String endpoint, String id) {

    // Assume that the hierarchy definitions are going to get compacted on the the next read
    // so that this function does as little work as possible

    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    String key = buildIdentifier(space, endpoint, id);
    String parentPath = buildEndpointKey(space, endpoint);

    // Remove the hierarchies related to this event

    invalidateHierarchy(id, mutator, key, parentPath, entityVersionsTemplate, ENTITY_ID_DIGESTS_CF, ENTITY_ID_BUCKETS_CF);
    invalidateHierarchy(id, mutator, key, parentPath, userDefinedAttributesTemplate, USER_DEFINED_DIGESTS_CF, USER_DEFINED_BUCKETS_CF);

    // Delete the raw data pertaining to this event

    mutator.addDeletion(key, ENTITY_VERSIONS_CF);
    mutator.addDeletion(key, USER_DEFINED_ATTRIBUTES_CF);

    mutator.execute();

  }

  private void invalidateHierarchy(String id, Mutator<String> mutator, String key, String parentPath, ColumnFamilyTemplate<String, String> template, String digestsCF, String bucketsCF) {

    ColumnFamilyResult<String,String> result = template.queryColumns(key);

    if (result.hasResults()) {
      String entityIdPartition = result.getString(PARTITION_KEY);

      // Invalidate the digest hierarchy to the bucket that was just deleted

      MerkleNode entityIdParentNode = MerkleNode.buildHierarchy(entityIdPartition);
      String entityIdBucketKey = deleteDescendencyPath(parentPath, entityIdParentNode, mutator, digestsCF);

      // Delete the item level digest from the buckets CF

      mutator.addDeletion(entityIdBucketKey, bucketsCF, id, StringSerializer.get());

    }
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

    String qualifiedBucketName = qualifyNodeName(parentName, node);

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

      mutator.addInsertion(qualifiedBucketName, hierarchyDigestCF, HFactory.createStringColumn(DIGEST_KEY,""));

      recordLineage(qualifiedBucketName, node.getChild(), mutator, hierarchyDigests, bucketCF, hierarchyCF, hierarchyDigestCF);
    }

  }

  private String qualifyNodeName(String parentName, MerkleNode node) {
    String qualifiedBucketName;

    if (node.getName().isEmpty()) {
      qualifiedBucketName = parentName;
    } else {
      qualifiedBucketName = parentName + "." + node.getName();
    }
    return qualifiedBucketName;
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


  private String buildIdentifier(Long space, String endpoint, String id) {
    return KEY_JOINER.join(space, endpoint, id);
  }

  private String buildEndpointKey(Long space, String endpoint) {
    return KEY_JOINER.join(space, endpoint);
  }


}
