package net.lshift.diffa.versioning;


import com.ecyrd.speed4j.StopWatch;
import com.ecyrd.speed4j.StopWatchFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import net.lshift.diffa.adapter.scanning.*;
import net.lshift.diffa.versioning.plumbing.BucketWriter;
import net.lshift.diffa.versioning.plumbing.DeltaBucketWriter;
import net.lshift.diffa.versioning.plumbing.OutrightBucketWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraVersionStore implements VersionStore {

  static Logger log = LoggerFactory.getLogger(CassandraVersionStore.class);

  StopWatchFactory stopWatchFactory = StopWatchFactory.getInstance("loggingFactory");

  static final int UNLIMITED_SLICE_SIZE = -1;
  static final String MAX_SLICE_SIZE_KEY = "max.slice.size";

  static final Joiner KEY_JOINER = Joiner.on(".").skipNulls();

  static final String KEY_SPACE = "version_store";
  static final String NULL_MD5 = "d41d8cd98f00b204e9800998ecf8427e";

  /**
   * Storage for the raw entity version and their raw partitioning attributes (should they have been supplied)
   */
  static final String ENTITY_VERSIONS_CF = "entity_versions";
  @Deprecated static final String DIRTY_ENTITIES_CF = "dirty_entities";
  static final String USER_DEFINED_ATTRIBUTES_CF = "user_defined_attributes";
  static final String PAIR_DELTA_CF = "pair_delta";

  /**
   * Storage for Merkle trees for each endpoint using the (optional) user defined partitioning attribute(s)
   */
  static final String USER_DEFINED_BUCKETS_CF = "user_defined_buckets";
  static final String USER_DEFINED_DIGESTS_CF = "user_defined_digests";
  static final String USER_DEFINED_HIERARCHY_CF = "user_defined_hierarchy";

  /**
   * Storage for Merkle trees for each pair using the (mandatory) entity id field as a partitioning attribute
   */
  static final String PAIR_BUCKETS_CF = "pair_buckets";
  static final String PAIR_DIGESTS_CF = "pair_digests";
  static final String PAIR_HIERARCHY_CF = "pair_hierarchy";

  /**
   * Storage for Merkle trees for the delta between two pairs using the (mandatory) entity id field as a partitioning attribute
   */
  static final String ENTITY_ID_BUCKETS_CF = "entity_id_buckets";
  static final String ENTITY_ID_DIGESTS_CF = "entity_id_digests";
  static final String ENTITY_ID_HIERARCHY_CF = "entity_id_hierarchy";


  /**
   * Commonly used constants in columns
   */

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

  private ColumnFamilyTemplate<String, String> pairDigestsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, PAIR_DIGESTS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  private Map<Long,Integer> sliceSizes = new ConcurrentHashMap<Long,Integer>();

  public void addEvent(Long endpoint, PartitionedEvent event) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    final String id = buildIdentifier(endpoint, event.getId());
    final String parentPath = endpoint.toString();

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    mutator.insertColumn(id, ENTITY_VERSIONS_CF, VERSION_KEY, event.getVersion() );

    if (event.getLastUpdated() != null) {
      mutator.insertDateColumn(id, ENTITY_VERSIONS_CF, LAST_UPDATE_KEY, event.getLastUpdated());
    }

    String entityIdPartition = event.getIdHierarchy().getDescendencyPath();
    mutator.insertColumn(id, ENTITY_VERSIONS_CF,  PARTITION_KEY, entityIdPartition);

    // Update the dirty entities for a subsequent re-sync to avoid
    // having to traverse buckets in order to pair-off changes between two
    // endpoints.
    String dirtyKey = buildIdentifier(endpoint, entityIdPartition);
    mutator.invalidateColumn(dirtyKey, DIRTY_ENTITIES_CF, event.getId());

    MerkleNode entityIdRootNode = new MerkleNode("", event.getIdHierarchy());

    List<String> viewsToUpdate = eventSubscribers(endpoint, event);

    for(String view : viewsToUpdate) {
      // TODO Work out how to include the view
      BucketWriter writer = new OutrightBucketWriter(mutator, ENTITY_ID_BUCKETS_CF);
      recordLineage(parentPath, entityIdRootNode, mutator, entityIdDigestsTemplate, writer, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF);
    }

    Map<String, String> attributes = event.getAttributes();
    if (attributes != null && !attributes.isEmpty()) {

      for(Map.Entry<String,String> entry : attributes.entrySet()) {
        mutator.insertColumn(id, USER_DEFINED_ATTRIBUTES_CF, entry.getKey(), entry.getValue());
      }

      String userDefinedPartition = event.getAttributeHierarchy().getDescendencyPath();
      mutator.insertColumn(id, USER_DEFINED_ATTRIBUTES_CF, PARTITION_KEY, userDefinedPartition);

      MerkleNode userDefinedRootNode = new MerkleNode("", event.getAttributeHierarchy());
      BucketWriter writer = new OutrightBucketWriter(mutator, USER_DEFINED_BUCKETS_CF);
      recordLineage(parentPath, userDefinedRootNode, mutator, userDefinedDigestsTemplate, writer, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF);
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

    mutator.deleteRow(key, ENTITY_VERSIONS_CF);
    mutator.deleteRow(key, ENTITY_ID_BUCKETS_CF);
    mutator.deleteRow(key, USER_DEFINED_ATTRIBUTES_CF);

    // Mark the entity as dirty so that subsequent compactions had emit match events based on this
    MerkleNode dirtyNode = MerkleUtils.buildEntityIdNode(id, null);
    final String dirtyKey = buildIdentifier(endpoint, dirtyNode.getDescendencyPath());
    mutator.invalidateColumn(dirtyKey, DIRTY_ENTITIES_CF, id);

    mutator.execute();

    stopWatch.stop(String.format("deleteEvent: endpoint = %s / event = %s", endpoint, id));

  }

  public SortedMap<String,BucketDigest> getEntityIdDigests(Long endpoint) {
    return getEntityIdDigests(endpoint, null);
  }

  public SortedMap<String,BucketDigest> getEntityIdDigests(Long endpoint, String bucketName) {
    return getGenericDigests(endpoint, bucketName, entityIdDigestsTemplate, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF, ENTITY_ID_BUCKETS_CF, UNLIMITED_SLICE_SIZE);
  }

  public SortedMap<String,BucketDigest> getEntityIdDigests(Long endpoint, String bucketName, boolean isLeaf) {
    return getGenericDigests(endpoint, bucketName, isLeaf, entityIdDigestsTemplate, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF, ENTITY_ID_BUCKETS_CF, UNLIMITED_SLICE_SIZE);
  }

  public SortedMap<String,BucketDigest> getUserDefinedDigests(Long endpoint, String bucketName, int maxSliceSize) {
    return getGenericDigests2(endpoint, bucketName, false, userDefinedDigestsTemplate, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF, USER_DEFINED_BUCKETS_CF, maxSliceSize);
  }

  @Deprecated public List<EntityDifference> flatComparison(Long left, Long right) {
    return compare(left, right, null, false);
  }

  public void setMaxSliceSize(Long endpoint, int size) {
    sliceSizes.put(endpoint, size);
  }

  public void delta(PairProjection view) {
    String bucket = "";
    boolean isLeaf = false;
    deltify(view.getLeft(), view.getRight(), bucket);
  }

  public TreeLevelRollup getMaterializedDelta(PairProjection view) {
    return getMaterializedDelta(view, null);
  }

  public TreeLevelRollup getMaterializedDelta(PairProjection view, String bucket) {

    int maxSliceSize = 100; // TODO hard coded

    String key = KEY_JOINER.join(view.getLeft(), view.getRight(), bucket);

    return getChildDigests(key, pairDigestsTemplate, PAIR_HIERARCHY_CF, PAIR_DIGESTS_CF, PAIR_BUCKETS_CF, maxSliceSize);
  }

  public List<ScanRequest> continueInterview(
      Long endpoint,
      Set<ScanConstraint> constraints,
      Set<ScanAggregation> aggregations,
      Set<ScanResultEntry> entries) {


    if (entries == null || entries.isEmpty()) {

      throw new RuntimeException("Think about how to handle this ... ");

    } else {

      if (aggregations != null) {

        for (ScanAggregation sa : aggregations) {
          if (sa instanceof DateAggregation) {
            DateAggregation dateAggregation = (DateAggregation) sa;
            DateGranularityEnum g = dateAggregation.getGranularity();

            if (g == DateGranularityEnum.Yearly) {

              int maxSliceSize = getSliceSize(endpoint);
              String key = endpoint + "";
              TreeLevelRollup qualified = getChildDigests(key, userDefinedDigestsTemplate, USER_DEFINED_HIERARCHY_CF, USER_DEFINED_DIGESTS_CF, USER_DEFINED_BUCKETS_CF, maxSliceSize);
              TreeLevelRollup unqualified = unqualify(endpoint, qualified);

              Map<String,BucketDigest> remote = DifferencingUtils.convertAggregates(entries);

              MapDifference<String,BucketDigest> d = Maps.difference(remote,unqualified.getMembers());

              if (d.areEqual()) {
                return new ArrayList<ScanRequest>();
              }

              else {
                List<ScanRequest> r = new ArrayList<ScanRequest>();

                ScanRequest re = new ScanRequest(constraints, aggregations);

                r.add(re);

                return r;
              }
            }
          }
        }

      }

    }



    return null;
  }

  //////////////////////////////////////////////////////
  // Internal plumbing
  //////////////////////////////////////////////////////

  /**
   * This returns the list of views that requires an entity based tree (i.e. for each pair.)
   */
  private List<String> eventSubscribers(Long endpoint, PartitionedEvent event) {
    List<String> views = new ArrayList<String>();

    // TODO Hack - make sure that the caller indexes at least once (for now) in the for loop that this is called
    views.add("");

    return views;
  }

  private int getSliceSize(Long endpoint) {

    Integer sliceSize = sliceSizes.get(endpoint);

    if (sliceSize == null) {
      return UNLIMITED_SLICE_SIZE;
    }
    else {
      return sliceSize;
    }
  }

  private void deltify(Long left, Long right, String bucket) {
    TreeLevelDifference treeLevelDifference = getDifference(left, right, bucket);
    BatchMutator mutator = new BasicBatchMutator(keyspace);
    establishDifferingSides(left, right, treeLevelDifference, mutator);
    mutator.execute();
  }

  private TreeLevelDifference getDifference(Long left, Long right, String bucket) {
    TreeLevelRollup leftTree = getEntityIdChildDigests(left, bucket);
    TreeLevelRollup rightTree = getEntityIdChildDigests(right, bucket);
    MapDifference<String, BucketDigest> difference = Maps.difference(leftTree.getMembers(), rightTree.getMembers());

    if (leftTree.isLeaf() == rightTree.isLeaf()) {
      return new TreeLevelDifference(difference, leftTree.isLeaf());
    }
    else {
      throw new RuntimeException("Inconsistent leafyness; left [ " + leftTree + " ]; right [ " + rightTree + " ]");
    }

  }

  private List<EntityDifference> compare(Long left, Long right, String bucket, boolean isLeaf) {

    List<EntityDifference> diffs = new ArrayList<EntityDifference>();

    SortedMap<String,BucketDigest> leftDigests = maybeGetEntityIdDigests(left, bucket, isLeaf);
    SortedMap<String,BucketDigest> rightDigests = maybeGetEntityIdDigests(right, bucket, isLeaf);

    // TODO figure out what happens with empty endpoints, i.e. when and if an empty map is returned ....

    MapDifference<String,BucketDigest> d = Maps.difference(leftDigests, rightDigests);

    if ( ! d.areEqual() ) {
      /*
      Map<String, MapDifference.ValueDifference<BucketDigest>> differing = d.entriesDiffering();
      List<EntityDifference> bothDifferent = establishDifferingSides(left, right, null, isLeaf);
      diffs.addAll(bothDifferent);

      Map<String,BucketDigest> lhs = d.entriesOnlyOnLeft();
      List<EntityDifference> lhsDiffs = establishMissingSide(left, right, lhs);
      diffs.addAll(lhsDiffs);

      Map<String,BucketDigest> rhs = d.entriesOnlyOnRight();
      List<EntityDifference> rhsDiffs = establishMissingSide(left, right, rhs);
      diffs.addAll(rhsDiffs);
      */
    }

    return diffs;

  }

  /**
   * Hack wrapper so that getting digests for a non-existent key does not blow up.
   * It might be better to not throw the exception in the first place.
   */
  private SortedMap<String,BucketDigest> maybeGetEntityIdDigests(Long endpoint, String bucketName, boolean isLeaf) {

    SortedMap<String,BucketDigest> digests;

    try {
      digests = getEntityIdDigests(endpoint, bucketName, isLeaf);
    }
    catch (BucketNotFoundException e) {
      digests = new TreeMap<String, BucketDigest>();
    }

    return digests;
  }

  private void establishDifferingSides(Long left, Long right, TreeLevelDifference currentTreeLevelDiff, BatchMutator mutator) {

    MapDifference<String,BucketDigest> diffs = currentTreeLevelDiff.getDifference();
    Map<String, MapDifference.ValueDifference<BucketDigest>> differing = diffs.entriesDiffering();

    for (String bucket : differing.keySet()) {

      if (currentTreeLevelDiff.isLeaf()) {
        establishBucketDifferences(left, right, bucket, mutator);
      }
      else {
        TreeLevelDifference nextTreeLevelDiff = getDifference(left, right, bucket);
        establishDifferingSides(left, right, nextTreeLevelDiff, mutator);
      }

    }

    Map<String,BucketDigest> onlyOnLeft = diffs.entriesOnlyOnLeft();
    establishMissingSide(left, onlyOnLeft, mutator);

    Map<String,BucketDigest> onlyOnRight = diffs.entriesOnlyOnRight();
    establishMissingSide(right, onlyOnRight, mutator);

  }

  private void establishMissingSide(Long side, Map<String,BucketDigest> missingSide, BatchMutator mutator) {

    for (String bucket : missingSide.keySet()) {

      BucketDigest bucketDigest = missingSide.get(bucket);

      if (bucketDigest.isLeaf()) {

        establishBucketDifferences(side, bucket, mutator);

      } else {

        TreeLevelRollup subTree = getEntityIdChildDigests(side, bucket);
        Map<String,BucketDigest> members = subTree.getMembers();
        establishMissingSide(side, members, mutator);
      }
    }
  }

  private TreeLevelRollup getEntityIdChildDigests(Long endpoint, String bucket) {
    String key = buildKeyFromBucketName(endpoint, bucket);
    TreeLevelRollup qualified =  getChildDigests(key, entityIdDigestsTemplate, ENTITY_ID_HIERARCHY_CF, ENTITY_ID_DIGESTS_CF, ENTITY_ID_BUCKETS_CF);
    return unqualify(endpoint, qualified);
  }

  /*
  private Map<String,Boolean> getChildren(Long endpoint, String bucket, String hierarchyCF) {
    Map<String,Boolean> children = new HashMap<String, Boolean>();

    final String key = buildKeyFromBucketName(endpoint, bucket);

    ColumnSliceIterator<String, String, Boolean> hierarchyIterator = getHierarchyIterator(key, hierarchyCF);

    while (hierarchyIterator.hasNext()) {
      HColumn<String, Boolean> column = hierarchyIterator.next();
      String child = column.getName();
      boolean isLeaf = column.getValue();
      children.put(child, isLeaf);
    }

    return children;
  }
  */


  private void establishBucketDifferences(Long side, String key, BatchMutator mutator) {
    establishBucketDifferences(side, null, key , mutator);
  }

  private void establishBucketDifferences(Long left, Long right, String key, BatchMutator mutator) {
    Map<String, EntityDifference> potentialDiffs = new HashMap<String,EntityDifference>();

    // Read out the dirty entities by looking at the LHS first
    // Once we have the established the differences from the LHS perspective,
    // do the same for the RHS, but don't attempt to re-establish diffs that were detected
    // during the LHS processing

    if (left != null) {

      ColumnSliceIterator<String, String, String> leftIterator = getEntityIdBucketIterator(left, key);

      while (leftIterator.hasNext()) {
        HColumn<String,String> entityColumn = leftIterator.next();
        final String entityId = entityColumn.getName();
        EntityDifference potentialDiff = getPotentialDifference(left, right, entityId);
        potentialDiffs.put(entityId, potentialDiff);
      }

    }

    if (right != null) {

      ColumnSliceIterator<String, String, String> rightIterator = getEntityIdBucketIterator(right, key);

      while (rightIterator.hasNext()) {
        HColumn<String,String> entityColumn = rightIterator.next();
        final String entityId = entityColumn.getName();

        if (! potentialDiffs.containsKey(entityId) ) {

          EntityDifference potentialDiff = getPotentialDifference(left, right, entityId);
          potentialDiffs.put(entityId, potentialDiff);
        }

      }

    }

    Predicate<EntityDifference> diffFilter = new Predicate<EntityDifference>() {
      @Override
      public boolean apply(EntityDifference input) {
        return input.isDifferent();
      }
    };

    Map<String,EntityDifference> filtered = Maps.filterValues(potentialDiffs, diffFilter);

    if (!filtered.isEmpty()) {

      String parentPath = KEY_JOINER.join(left, right);//buildKeyFromBucketName(left, right, key);

      for(Map.Entry<String,EntityDifference> entry : filtered.entrySet()) {
        EntityDifference diff = entry.getValue();
        MerkleNode entityIdNode = MerkleUtils.buildEntityIdNode(diff.getId(), null);
        MerkleNode rootNode = new MerkleNode("", entityIdNode);
        BucketWriter writer = new DeltaBucketWriter(PAIR_BUCKETS_CF, mutator, diff);
        recordLineage(parentPath, rootNode, mutator, pairDigestsTemplate, writer, PAIR_HIERARCHY_CF, PAIR_DIGESTS_CF);
        //asd
      }

    }

  }

  private EntityDifference getPotentialDifference(Long left, Long right, String id) {
    final String leftVersion = getEntityVersion(left, id);
    final String rightVersion = getEntityVersion(right, id);
    return new EntityDifference(id, leftVersion, rightVersion);
  }

  private ColumnSliceIterator<String, String, String> getDirtyIterator(Long endpoint, String key) {

    final String queryKey = buildIdentifier(endpoint, key);

    SliceQuery<String,String,String> query =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    query.setColumnFamily(DIRTY_ENTITIES_CF);
    query.setKey(queryKey);

    return new ColumnSliceIterator<String,String,String>(query, "", "",false);
  }

  private ColumnSliceIterator<String, String, String> getEntityIdBucketIterator(Long endpoint, String key) {

    final String queryKey = buildIdentifier(endpoint, key);

    SliceQuery<String,String,String> query =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    query.setColumnFamily(ENTITY_ID_BUCKETS_CF);
    query.setKey(queryKey);

    return new ColumnSliceIterator<String,String,String>(query, "", "",false);
  }

  private String getEntityVersion(Long endpoint, String key) {

    if (endpoint == null || key == null) {
      return null;
    }
    else {

      final String entityKey = buildIdentifier(endpoint, key);
      ColumnFamilyResult<String,String> result = entityVersionsTemplate.queryColumns(entityKey);

      if (result.hasResults()) {
        return result.getString(VERSION_KEY);
      }
      else {
        return null;
      }
    }

  }

  private String deleteDescendencyPath(String parentPath, MerkleNode node, BatchMutator mutator, String digestCF) {
    String key = qualifyNodeName(parentPath, node);

    mutator.invalidateColumn(key, digestCF, DIGEST_KEY);

    if (node.isLeaf()) {
      return key;
    }
    else {
      return deleteDescendencyPath(key, node.getChild(), mutator, digestCF);
    }
  }

  private void invalidateHierarchy(String id, BatchMutator mutator, String key, String parentPath, ColumnFamilyTemplate<String, String> template, String digestsCF, String bucketsCF) {

    ColumnFamilyResult<String,String> result = template.queryColumns(key);

    if (result.hasResults()) {
      String entityIdPartition = result.getString(PARTITION_KEY);

      // Invalidate the digest hierarchy to the bucket that was just deleted

      MerkleNode entityIdParentNode = MerkleNode.buildHierarchy(entityIdPartition);
      String entityIdBucketKey = deleteDescendencyPath(parentPath, entityIdParentNode, mutator, digestsCF);

      // Delete the item level digest from the buckets CF

      mutator.deleteColumn(entityIdBucketKey, bucketsCF, id);
    }
  }

  private void recordLineage(String parentName, MerkleNode node, BatchMutator mutator, ColumnFamilyTemplate<String,String> hierarchyDigests, BucketWriter bucketWriter, String hierarchyCF, String hierarchyDigestCF) {

    String qualifiedBucketName = qualifyNodeName(parentName, node);

    // In any event, since we are updating a bucket in this lineage, we need to invalidate the digest of each
    // parent node

    mutator.invalidateColumn(qualifiedBucketName, hierarchyDigestCF, DIGEST_KEY);

    if (node.isLeaf()) {
      // This a leaf node so record the bucket value and it's parent
      bucketWriter.write(qualifiedBucketName, node);
      //mutator.insertColumn(qualifiedBucketName, bucketCF, node.getId(), node.getVersion());
    }
    else {

      MerkleNode child = node.getChild();
      HColumn<String,Boolean> childColumn = HFactory.createColumn(child.getName(), child.isLeaf(), StringSerializer.get(), BooleanSerializer.get());

      ColumnFamilyResult<String,String> result = hierarchyDigests.queryColumns(qualifiedBucketName);

      if (result.hasResults()) {
        String digestLabel = result.getString(child.getName());
        if (digestLabel == null || digestLabel.isEmpty()) {
          // TODO Consider populating the child value with the digest of the child instead of leaving it empty
          mutator.insertColumn(qualifiedBucketName, hierarchyCF, childColumn);
        }
      }
      else {
        // TODO See todo 4 lines back
        mutator.insertColumn(qualifiedBucketName, hierarchyCF, childColumn);

      }

      recordLineage(qualifiedBucketName, node.getChild(), mutator, hierarchyDigests, bucketWriter, hierarchyCF, hierarchyDigestCF);
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

  private SortedMap<String,BucketDigest> getGenericDigests(Long endpoint, String bucketName, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF, int maxSliceSize) {
    return getGenericDigests(endpoint, bucketName, false, hierarchyDigests, hierarchyCF, digestCF, bucketCF, maxSliceSize);
  }

  private SortedMap<String,BucketDigest> getGenericDigests(Long endpoint, String bucketName, boolean isLeaf, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF, int maxSliceSize) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    final String key = buildKeyFromBucketName(endpoint, bucketName);

    BucketDigest digest = getDigest(key, isLeaf, mutator, hierarchyDigests, hierarchyCF, digestCF, bucketCF, maxSliceSize);

    mutator.execute();

    stopWatch.stop(String.format("getEntityIdDigests: endpoint = %s / bucket = %s", endpoint, bucketName));

    SortedMap<String,BucketDigest> digests = new TreeMap<String, BucketDigest>();
    digests.put(key, digest);

    return unqualify(endpoint, digests);
  }

  private SortedMap<String,BucketDigest> getGenericDigests2(Long endpoint, String bucketName, boolean isLeaf, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF, int maxSliceSize) {

    StopWatch stopWatch = stopWatchFactory.getStopWatch();

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    final String key = buildKeyFromBucketName(endpoint, bucketName);

    BucketDigest digest = getDigest(key, isLeaf, mutator, hierarchyDigests, hierarchyCF, digestCF, bucketCF, maxSliceSize);

    mutator.execute();

    stopWatch.stop(String.format("getEntityIdDigests: endpoint = %s / bucket = %s", endpoint, bucketName));

    SortedMap<String,BucketDigest> digests = new TreeMap<String, BucketDigest>();
    digests.put(key, digest);

    return digests;
  }

  private String buildKeyFromBucketName(Long endpoint, String bucketName) {
    String key;
    if (bucketName == null || bucketName.isEmpty()) {
      key = endpoint.toString();
    }
    else {
      key = buildIdentifier(endpoint, bucketName);
    }
    return key;
  }

  private String buildKeyFromBucketName(Long left, Long right, String bucketName) {
    if (bucketName == null || bucketName.isEmpty()) {
      return KEY_JOINER.join(left, right);
    }
    else {
      return KEY_JOINER.join(left, right, bucketName);
    }
  }

  private TreeLevelRollup getChildDigests(String key, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF) {
    return getChildDigests(key, hierarchyDigests, hierarchyCF, digestCF, bucketCF, UNLIMITED_SLICE_SIZE);
  }

  private TreeLevelRollup getChildDigests(String key, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF, int maxSliceSize) {

    BatchMutator mutator = new BasicBatchMutator(keyspace);

    BitSet bits = new BitSet();
    int index = 0;
    Map<String,BucketDigest> digests = new HashMap<String,BucketDigest>();

    ColumnSliceIterator<String, String, Boolean> hierarchyIterator = getHierarchyIterator(key, hierarchyCF);

    while (hierarchyIterator.hasNext()) {
      HColumn<String, Boolean> hierarchyColumn = hierarchyIterator.next();

      String childName = hierarchyColumn.getName();
      boolean leaf = hierarchyColumn.getValue();

      if (leaf) {
        bits.set(index++);
      }

      String child = key + "." + childName;

      BucketDigest digest = getDigest(child, leaf, mutator, hierarchyDigests, hierarchyCF, digestCF, bucketCF, maxSliceSize);
      digests.put(child, digest);

    }

    mutator.execute();

    if (bits.cardinality() == 0 || bits.cardinality() == bits.length()) {

      boolean isLeaf = bits.get(0);
      return new TreeLevelRollup(digests, isLeaf);

    }
    else {
      throw new RuntimeException("Inconsistent sub tree: " + digests);
    }


  }


  private BucketDigest getDigest(String key, boolean isLeaf, BatchMutator mutator, ColumnFamilyTemplate<String, String> hierarchyDigests, String hierarchyCF, String digestCF, String bucketCF, int maxSliceSize) {

    //SortedMap<String,BucketDigest> digests = new TreeMap<String,BucketDigest>();
    ColumnFamilyResult<String, String> result = hierarchyDigests.queryColumns(key);

    if (isLeaf) {

      // Since this is a leaf node, we need to roll up all of the individual entity digests stored in the bucket CF

      BucketDigest bucketDigest = buildBucketDigest(key, bucketCF, maxSliceSize);

      // Check to see whether this bucket can be compacted

      if (NULL_MD5.equals(bucketDigest)) {

        // Delete since it is empty delete this bucket, digest and hierarchy
        mutator.deleteRow(key, bucketCF);
        deleteDigest(key, mutator, digestCF, hierarchyCF);

      } else {
        cacheDigest(key, mutator, digestCF, bucketDigest);

      }

      return bucketDigest;//digests.put(key, bucketDigest);

    }
    else if (result.hasResults()) {

      String cachedDigest = result.getString(DIGEST_KEY);

      if (cachedDigest == null || cachedDigest.isEmpty()) {

        // There is no cached digest for this key, we need to resolve the children and establish their digests

        ColumnSliceIterator<String, String, Boolean> hierarchyIterator = getHierarchyIterator(key, hierarchyCF);

        Digester digester = new Digester();

        while (hierarchyIterator.hasNext()) {
          HColumn<String, Boolean> hierarchyColumn = hierarchyIterator.next();

          String child = hierarchyColumn.getName();
          boolean leaf = hierarchyColumn.getValue();

          String qualifiedKey = key + "." + child;

          //SortedMap<String,BucketDigest> childDigests = getDigest(qualifiedKey, leaf, mutator, hierarchyDigests, hierarchyCF, digestCF, bucketCF);
          BucketDigest childDigest = getDigest(qualifiedKey, leaf, mutator, hierarchyDigests, hierarchyCF, digestCF, bucketCF, maxSliceSize);
          digester.addVersion(childDigest.getDigest());

          // We need to roll up the subtree digests to produce an over-arching digest for the current bucket

          /*for (Map.Entry<String, BucketDigest> entry : childDigests.entrySet()) {
            digester.addVersion(entry.getValue().getDigest());
          }*/
        }

        String digest = digester.getDigest();
        BucketDigest bucketDigest = new BucketDigest(key, digest, false);

        if (NULL_MD5.equals(bucketDigest)) {
          // Clean up empty digests
          deleteDigest(key, mutator, digestCF, hierarchyCF);
        }
        else {

          //digests.put(key, bucketDigest);

          // Don't forget to cache this digest for later use

          cacheDigest(key, mutator, digestCF, bucketDigest);

        }

        return bucketDigest;

      }
      else {
        BucketDigest bucketDigest = new BucketDigest(key, cachedDigest, false);
        //digests.put(key, bucketDigest);
        return bucketDigest;
      }

    } else {
      throw new BucketNotFoundException(key);
    }

    //return digests;
  }

  private ColumnSliceIterator<String, String, Boolean> getHierarchyIterator(String key, String hierarchyCF) {
    SliceQuery<String,String,Boolean> hierarchyQuery =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), BooleanSerializer.get());
    hierarchyQuery.setColumnFamily(hierarchyCF);
    hierarchyQuery.setKey(key);

    return new ColumnSliceIterator<String,String,Boolean>(hierarchyQuery, "", "",false);
  }

  private void cacheDigest(String key, BatchMutator mutator, String digestCF, BucketDigest bucketDigest) {
    mutator.insertColumn(key, digestCF, DIGEST_KEY, bucketDigest.getDigest());
  }

  private void deleteDigest(String key, BatchMutator mutator, String digestCF, String hierarchyCF) {
    mutator.deleteRow(key, digestCF);
    int pivot = key.lastIndexOf(".");
    String parent = key.substring(0, pivot - 1);
    String child = key.substring(pivot + 1);
    mutator.deleteColumn(parent, hierarchyCF, child);
  }

  private BucketDigest buildBucketDigest(String key, String bucketCF, final int maxSliceSize) {

    List<String> digests = new ArrayList<String>();

    SliceQuery<String,String,String> query =  HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

    query.setColumnFamily(bucketCF);
    query.setKey(key);

    ColumnSliceIterator<String,String,String> bucketIterator = new ColumnSliceIterator<String,String,String>(query, "", "",false);

    // Slice the buckets up according to the maximum sub bucket size

    int currentBucketSize = 0;
    Digester digester = new Digester();

    while (bucketIterator.hasNext()) {
      HColumn<String, String> column = bucketIterator.next();
      String version = column.getValue();

      if (maxSliceSize > 0 && currentBucketSize == maxSliceSize) {
        rolloverSlice(digests, digester);
        currentBucketSize = 0;
      }

      digester.addVersion(version);

      if (maxSliceSize > 0) {
        currentBucketSize++;
      }

    }

    rolloverSlice(digests, digester);

    for (String digest : digests) {
      digester.addVersion(digest);
    }

    return new BucketDigest(key, digester.getDigest(), true);
  }

  /**
   * This is a hack to overcome the fact that the digest tree builder (unhelpfully) qualifies each node
   * by prefixing the endpoint id to the key name. This oversight makes subsequent tree comparisons difficult,
   * because each tree will have differing root names, which is uncool.
   *
   * Rather than fixing the underlying issue, at this point I am just going to rewrite each node label to
   * get rid of this prefixing.
   */
  private SortedMap<String, BucketDigest> unqualify(Long endpoint, Map<String, BucketDigest> qualified) {
    SortedMap<String,BucketDigest> unqualified = new TreeMap<String, BucketDigest>();

    if (qualified != null) {

      for(Map.Entry<String,BucketDigest> entry : qualified.entrySet()) {
        String prefix = endpoint + "(\\.)?";
        String rewrittenKey = entry.getKey().replaceFirst(prefix, "");
        unqualified.put(rewrittenKey, entry.getValue());
      }

    }

    return unqualified;
  }

  private TreeLevelRollup unqualify(Long endpoint, TreeLevelRollup qualified) {
    SortedMap<String,BucketDigest> unqualified = unqualify(endpoint, qualified.getMembers());
    return new TreeLevelRollup(unqualified, qualified.isLeaf());
  }
  /*
  private Map<String, String> unqualify(Long endpoint, Map<String, BucketDigest> qualified) {
    SortedMap<String,String> unqualified = new TreeMap<String, String>();

    for(Map.Entry<String,String> entry : qualified.entrySet()) {
      String prefix = endpoint + "(\\.)?";
      String rewrittenKey = entry.getKey().replaceFirst(prefix, "");
      unqualified.put(rewrittenKey, entry.getValue());
    }

    return unqualified;
  }
  */

  private void rolloverSlice(List<String> digests, Digester digester) {
    String currentDigest = digester.getDigest();
    digests.add(currentDigest);
    digester.reset();
  }


  private String buildIdentifier(Long endpoint, String id) {
    return KEY_JOINER.join(endpoint, id);
  }

}
