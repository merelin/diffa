package net.lshift.diffa.versioning;


import com.ecyrd.speed4j.StopWatch;
import com.ecyrd.speed4j.StopWatchFactory;
import com.google.common.base.Joiner;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
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

  StopWatchFactory stopWatchFactory = StopWatchFactory.getInstance("loggingFactory");

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

  private ColumnFamilyTemplate<String, String> entityIdDigestsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, ENTITY_ID_DIGESTS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  private ColumnFamilyTemplate<String, String> userDefinedDigestsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, USER_DEFINED_DIGESTS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  private ColumnFamilyTemplate<String, String> userDefinedAttributesTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, USER_DEFINED_ATTRIBUTES_CF,
          StringSerializer.get(),
          StringSerializer.get());

  public void addEvent(Long endpoint, PartitionedEvent event) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    final String id = buildIdentifier(endpoint, event.getId());
    final String parentPath = endpoint.toString();

    //Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    //mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createStringColumn(VERSION_KEY, event.getVersion()));
    mutator.insertColumn(id, ENTITY_VERSIONS_CF, VERSION_KEY, event.getVersion() );

    if (event.getLastUpdated() != null) {
      // TODO use a joda serializer instead of converting to java.util.Date
      //Date date = event.getLastUpdated().toDate();
      //mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createColumn(LAST_UPDATE_KEY, date, StringSerializer.get(), DateSerializer.get()));
      mutator.insertDateColumn(id, ENTITY_VERSIONS_CF, LAST_UPDATE_KEY, event.getLastUpdated());
    }

    String entityIdPartition = event.getIdHierarchy().getDescendencyPath();
    //mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createStringColumn(PARTITION_KEY, entityIdPartition));
    mutator.insertColumn(id, ENTITY_VERSIONS_CF,  PARTITION_KEY, entityIdPartition);

    MerkleNode entityIdRootNode = new MerkleNode("", event.getIdHierarchy());

    recordLineage(parentPath, entityIdRootNode, mutator, entityIdDigestsTemplate, ENTITY_ID_BUCKETS_CF, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);

    Map<String, String> attributes = event.getAttributes();
    if (attributes != null && !attributes.isEmpty()) {

      for(Map.Entry<String,String> entry : attributes.entrySet()) {
        //mutator.addInsertion(id, USER_DEFINED_ATTRIBUTES_CF, HFactory.createStringColumn(entry.getKey(), entry.getValue()));
        mutator.insertColumn(id, USER_DEFINED_ATTRIBUTES_CF, entry.getKey(), entry.getValue());
      }

      String userDefinedPartition = event.getAttributeHierarchy().getDescendencyPath();
      //mutator.addInsertion(id, USER_DEFINED_ATTRIBUTES_CF, HFactory.createStringColumn(PARTITION_KEY, userDefinedPartition));
      mutator.insertColumn(id, USER_DEFINED_ATTRIBUTES_CF, PARTITION_KEY, userDefinedPartition);

      MerkleNode userDefinedRootNode = new MerkleNode("", event.getAttributeHierarchy());
      recordLineage(parentPath, userDefinedRootNode, mutator, userDefinedDigestsTemplate, USER_DEFINED_BUCKETS_CF, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
    }

    mutator.execute();

    stopWatch.stop(String.format("addEvent: endpoint = %s / event = %s", endpoint, event.getId()));

  }

  public void deleteEvent(Long endpoint, String id) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    // Assume that the hierarchy definitions are going to get compacted on the the next read
    // so that this function does as little work as possible

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    final String key = buildIdentifier(endpoint, id);
    final String parentPath = endpoint.toString();

    // Remove the hierarchies related to this event

    invalidateHierarchy(id, mutator, key, parentPath, entityVersionsTemplate, ENTITY_ID_DIGESTS_CF, ENTITY_ID_BUCKETS_CF);
    invalidateHierarchy(id, mutator, key, parentPath, userDefinedAttributesTemplate, USER_DEFINED_DIGESTS_CF, USER_DEFINED_BUCKETS_CF);

    // Delete the raw data pertaining to this event

    //mutator.addDeletion(key, ENTITY_VERSIONS_CF);
    mutator.deleteRow(key, ENTITY_ID_BUCKETS_CF);
    //mutator.addDeletion(key, USER_DEFINED_ATTRIBUTES_CF);
    mutator.deleteRow(key, USER_DEFINED_ATTRIBUTES_CF);

    mutator.execute();

    stopWatch.stop(String.format("deleteEvent: endpoint = %s / event = %s", endpoint, id));

  }

  public SortedMap<String,String> getEntityIdDigests(Long endpoint) {
    return getEntityIdDigests(endpoint, null);
  }

  public SortedMap<String,String> getEntityIdDigests(Long endpoint, String bucketName) {
    return getGenericDigests(endpoint, bucketName, entityIdDigestsTemplate, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);
  }

  public SortedMap<String, String> getUserDefinedDigests(Long endpoint) {
    return getUserDefinedDigests(endpoint, null);
  }

  public SortedMap<String,String> getUserDefinedDigests(Long endpoint, String bucketName) {
    return getGenericDigests(endpoint, bucketName, userDefinedDigestsTemplate, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
  }

  //////////////////////////////////////////////////////
  // Internal plumbing
  //////////////////////////////////////////////////////

  private String deleteDescendencyPath(String parentPath, MerkleNode node, BatchMutator mutator, String digestCF) {
    String key = qualifyNodeName(parentPath, node);

    //mutator.addInsertion(key, digestCF, HFactory.createStringColumn(DIGEST_KEY, ""));
    mutator.invalidateColumn(key, digestCF, DIGEST_KEY);

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

  private void invalidateHierarchy(String id, BatchMutator mutator, String key, String parentPath, ColumnFamilyTemplate<String, String> template, String digestsCF, String bucketsCF) {

    ColumnFamilyResult<String,String> result = template.queryColumns(key);

    if (result.hasResults()) {
      String entityIdPartition = result.getString(PARTITION_KEY);

      // Invalidate the digest hierarchy to the bucket that was just deleted

      MerkleNode entityIdParentNode = MerkleNode.buildHierarchy(entityIdPartition);
      String entityIdBucketKey = deleteDescendencyPath(parentPath, entityIdParentNode, mutator, digestsCF);

      // Delete the item level digest from the buckets CF

      //mutator.addDeletion(entityIdBucketKey, bucketsCF, id, StringSerializer.get());
      mutator.deleteColumn(entityIdBucketKey, bucketsCF, id);
    }
  }

  private void recordLineage(String parentName, MerkleNode node, BatchMutator mutator, ColumnFamilyTemplate<String,String> hierarchyDigests, String bucketCF, String hierarchyCF, String hierarchyDigestCF) {

    String qualifiedBucketName = qualifyNodeName(parentName, node);

    // In any event, since we are updating a bucket in this lineage, we need to invalidate the digest of each
    // parent node

    //mutator.addInsertion(qualifiedBucketName, hierarchyDigestCF, HFactory.createStringColumn(DIGEST_KEY,""));
    mutator.invalidateColumn(qualifiedBucketName, hierarchyDigestCF, DIGEST_KEY);

    if (node.isLeaf()) {
      // This a leaf node so record the bucket value and it's parent

      //mutator.addInsertion(qualifiedBucketName, bucketCF, HFactory.createStringColumn(node.getId(), node.getVersion()));
      mutator.insertColumn(qualifiedBucketName, bucketCF, node.getId(), node.getVersion());
    }
    else {

      MerkleNode child = node.getChild();
      HColumn<String,Boolean> childColumn = HFactory.createColumn(child.getName(), child.isLeaf(), StringSerializer.get(), BooleanSerializer.get());

      ColumnFamilyResult<String,String> result = hierarchyDigests.queryColumns(qualifiedBucketName);

      if (result.hasResults()) {
        String digestLabel = result.getString(child.getName());
        if (digestLabel == null || digestLabel.isEmpty()) {
          // TODO Consider populating the child value with the digest of the child instead of leaving it empty
          //mutator.addInsertion(qualifiedBucketName, hierarchyCF, childColumn);
          mutator.insertColumn(qualifiedBucketName, hierarchyCF, childColumn);
        }
      }
      else {
        // TODO See todo 4 lines back
        //mutator.addInsertion(qualifiedBucketName, hierarchyCF, childColumn);
        mutator.insertColumn(qualifiedBucketName, hierarchyCF, childColumn);

      }



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

  private SortedMap<String,String> getGenericDigests(Long endpoint, String bucketName, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String hierarchyDigestCF) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    final String key;

    if (bucketName == null || bucketName.isEmpty()) {
      key = endpoint.toString();
    }
    else {
      key = buildIdentifier(endpoint, bucketName);
    }

    SortedMap<String,String> digest = getDigests(key, false, mutator, hierarchyDigests, hierarchyCF, hierarchyDigestCF);

    mutator.execute();

    stopWatch.stop(String.format("getEntityIdDigests: endpoint = %s / bucket = %s", endpoint, bucketName));

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

        Digester digester = new Digester();

        while (hierarchyIterator.hasNext()) {
          HColumn<String, Boolean> hierarchyColumn = hierarchyIterator.next();

          String child = hierarchyColumn.getName();
          boolean leaf = hierarchyColumn.getValue();

          String qualifiedKey = key + "." + child;

          SortedMap<String,String> childDigests = getDigests(qualifiedKey, leaf, mutator, hierarchyDigests, hierarchyCF, hierarchyDigestCF);

          // We need to roll up the subtree digests to produce an over-arching digest for the current bucket





          for (Map.Entry<String, String> entry : childDigests.entrySet()) {
            digester.addVersion(entry.getValue());
            //System.err.println(key + ") add version: " + entry.getValue());
          }


        }

        String bucketDigest = digester.getDigest();
        digests.put(key, bucketDigest);


        //System.err.println(key + ") digest: " + bucketDigest);


        // Don't forget to cache this digest for later use

        cacheDigest(key, mutator, hierarchyDigestCF, bucketDigest);

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


  private String buildIdentifier(Long endpoint, String id) {
    return KEY_JOINER.join(endpoint, id);
  }

}
