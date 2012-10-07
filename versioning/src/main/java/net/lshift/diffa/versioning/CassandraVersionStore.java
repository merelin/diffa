package net.lshift.diffa.versioning;


import com.google.common.base.Joiner;
import com.google.common.collect.TreeMultiset;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.adapter.scanning.MissingAttributeException;
import net.lshift.diffa.adapter.scanning.ScanAggregation;

import javax.print.attribute.standard.JobName;
import java.util.*;

public class CassandraVersionStore implements VersionStore {

  static final Joiner KEY_JOINER = Joiner.on(".").skipNulls();
  static final Joiner ATTRIBUTES_JOINER = Joiner.on("_").skipNulls();

  static final String KEY_SPACE = "version_store";
  static final String ENTITY_VERSIONS_CF = "entity_versions";
  static final String ENTITY_ATTRIBUTES_CF = "entity_attributes";
  static final String USER_DEFINED_BUCKETS_CF = "user_defined_buckets";

  Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
  Keyspace keyspace = HFactory.createKeyspace(KEY_SPACE, cluster);

  ColumnFamilyTemplate<String, String> itemsTemplate =
      new ThriftColumnFamilyTemplate<String, String>(keyspace, ENTITY_VERSIONS_CF,
          StringSerializer.get(),
          StringSerializer.get());

  public void store(Long space, String endpoint, ChangeEvent event, Iterable<ScanAggregation> aggregations) {

    event.ensureContainsMandatoryFields();

    String id = buildIdentifier(space, endpoint, event.getId());

    Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

    mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createStringColumn("version", event.getVersion()));

    if (event.getLastUpdated() != null) {
      Date date = event.getLastUpdated().toDate();
      mutator.addInsertion(id, ENTITY_VERSIONS_CF,  HFactory.createColumn("lastUpdate", date, StringSerializer.get(), DateSerializer.get()));
    }

    Map<String, String> attributes = event.getAttributes();

    if (attributes != null && !attributes.isEmpty()) {

      for(Map.Entry<String,String> entry : attributes.entrySet()) {
        mutator.addInsertion(id, ENTITY_ATTRIBUTES_CF,  HFactory.createStringColumn(entry.getKey(), entry.getValue()));
      }

      String bucket = getBucketName(space, endpoint, event, aggregations);
      mutator.addInsertion(bucket, USER_DEFINED_BUCKETS_CF, HFactory.createStringColumn(event.getId(), event.getVersion()));

    }

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


}
