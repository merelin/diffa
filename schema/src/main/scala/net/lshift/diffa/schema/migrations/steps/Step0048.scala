/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.schema.migrations.steps

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types
import net.lshift.diffa.schema.migrations.{DefinePartitionInformationTable, VerifiedMigrationStep}
import scala.collection.JavaConversions._

object Step0048 extends VerifiedMigrationStep {

  def versionId = 48
  def name = "Sub space database layout"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    // Step 0 (part 1) - make sure we have some tables to record DB meta data

    migration.createTable("schema_version").
      column("version", Types.INTEGER, false).
      pk("version")

    // Step 0 (part 2) - include the partition info table on all DBs (support may be added in future)
    DefinePartitionInformationTable.defineTable(migration)

    // Begin creating the schema, so start with spaces, since lots of things depend on them

    migration.createTable("spaces").
      column("id", Types.BIGINT, false).
      column("name", Types.VARCHAR, 50, false).
      column("config_version", Types.INTEGER, 11, false).
      pk("id")

    // Now start to create things that are scoped on spaces

    migration.createTable("config_options").
      column("space", Types.BIGINT, false).
      column("opt_key", Types.VARCHAR, 255, false).
      column("opt_val", Types.VARCHAR, 255, true).
      pk("space", "opt_key")

    migration.alterTable("config_options").
      addForeignKey("fk_cfop_spcs", "space", "spaces", "id")

    migration.createTable("endpoints").
      column("space", Types.BIGINT, false).
      column("name", Types.VARCHAR, 50, false).
      column("scan_url", Types.VARCHAR, 1024, true).
      column("content_retrieval_url", Types.VARCHAR, 1024, true).
      column("version_generation_url", Types.VARCHAR, 1024, true).
      column("inbound_url", Types.VARCHAR, 1024, true).
      column("collation_type", Types.VARCHAR, 16, false, "ascii").
      pk("space", "name")

    migration.alterTable("endpoints").
      addForeignKey("fk_edpt_spcs", "space", "spaces", "id")

    migration.createTable("endpoint_views").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      pk("space", "endpoint", "name")

    migration.alterTable("endpoint_views").
      addForeignKey("fk_epvw_edpt", Array("space", "endpoint"), "endpoints", Array("space", "name"))

    migration.createTable("pairs").
      column("space", Types.BIGINT, false).
      column("name", Types.VARCHAR, 50, false).
      column("upstream", Types.VARCHAR, 50, false).
      column("downstream", Types.VARCHAR, 50, false).
      column("version_policy_name", Types.VARCHAR, 50, true).
      column("matching_timeout", Types.INTEGER, true).
      column("scan_cron_spec", Types.VARCHAR, 50, true).
      column("allow_manual_scans", Types.BIT, 1, true, 0).
      column("scan_cron_enabled", Types.BIT, 1, true, 0).
      pk("space", "name")

    migration.alterTable("pairs").
      addForeignKey("fk_pair_spcs", "space", "spaces", "id").
      addForeignKey("fk_pair_upstream_edpt", Array("space", "upstream"), "endpoints", Array("space", "name")).
      addForeignKey("fk_pair_downstream_edpt", Array("space", "downstream"), "endpoints", Array("space", "name"))

    migration.createTable("escalations").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("action", Types.VARCHAR, 50, true).
      column("action_type", Types.VARCHAR, 255, false).
      column("delay", Types.INTEGER, 11, false, 0).
      column("rule", Types.VARCHAR, 1024, true, null).
      pk("space", "pair", "name")

    migration.alterTable("escalations").
      addForeignKey("fk_escl_pair", Array("space", "pair"), "pairs", Array("space", "pair"))

    migration.createTable("diffs").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("seq_id", Types.BIGINT, false).
      column("entity_id", Types.VARCHAR, 255, false).
      column("is_match", Types.BIT, false).
      column("detected_at", Types.TIMESTAMP, false).
      column("last_seen", Types.TIMESTAMP, false).
      column("upstream_vsn", Types.VARCHAR, 255, true).
      column("downstream_vsn", Types.VARCHAR, 255, true).
      column("ignored", Types.BIT, false).
      column("next_escalation", Types.VARCHAR, 50, true, null).
      column("next_escalation_time", Types.TIMESTAMP, true, null).
      pk("space", "pair", "seq_id")

    migration.alterTable("diffs")
      .addForeignKey("fk_diff_pair", Array("space", "pair"), "pairs", Array("space", "name"))

    migration.alterTable("diffs").
      addForeignKey("fk_next_esc", Array("space", "pair", "next_escalation"), "escalations", Array("space", "pair", "name"))

    migration.alterTable("diffs")
      .addUniqueConstraint("uk_diffs", "entity_id", "space", "pair")

    migration.createIndex("diff_last_seen", "diffs", "last_seen")
    migration.createIndex("diff_detection", "diffs", "detected_at")
    migration.createIndex("rdiff_is_matched", "diffs", "is_match")
    migration.createIndex("rdiff_domain_idx", "diffs", "entity_id", "space", "pair")


    migration.createTable("pending_diffs").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("seq_id", Types.BIGINT, false).
      column("entity_id", Types.VARCHAR, 255, false).
      column("detected_at", Types.TIMESTAMP, false).
      column("last_seen", Types.TIMESTAMP, false).
      column("upstream_vsn", Types.VARCHAR, 255, true).
      column("downstream_vsn", Types.VARCHAR, 255, true).
      pk("space", "pair", "seq_id")

    migration.alterTable("pending_diffs")
      .addForeignKey("fk_pddf_pair", Array("space", "pair"), "pairs", Array("space", "name"))

    migration.alterTable("pending_diffs")
      .addUniqueConstraint("uk_pending_diffs", "entity_id", "space", "pair")

    migration.createIndex("pdiff_domain_idx", "pending_diffs", "entity_id", "space", "pair")

    migration.createTable("pair_views").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("scan_cron_spec", Types.VARCHAR, 50, true).
      column("scan_cron_enabled", Types.BIT, 1, true, 0).
      pk("space", "pair", "name")

    migration.alterTable("pair_views").
      addForeignKey("fk_prvw_pair", Array("space", "pair"), "pairs", Array("space", "name"))


    migration.createTable("pair_reports").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("report_type", Types.VARCHAR, 50, false).
      column("target", Types.VARCHAR, 1024, false).
      pk("space", "pair", "name")

    migration.alterTable("pair_reports").
      addForeignKey("fk_prep_pair", Array("space", "pair"), "pairs", Array("space", "name"))

    migration.createTable("unique_category_names").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      pk("space", "endpoint", "name")

    migration.alterTable("unique_category_names").
      addForeignKey("fk_ucns_edpt", Array("space", "endpoint"), "endpoints", Array("space", "name"))

    migration.createTable("prefix_categories").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("space", "endpoint", "name")

    migration.alterTable("prefix_categories").
      addForeignKey("fk_pfcg_ucns", Array("space", "endpoint", "name"), "unique_category_names", Array("space", "endpoint", "name"))

    migration.createTable("set_categories").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      pk("space", "endpoint", "name", "value")

    migration.alterTable("set_categories").
      addForeignKey("fk_stcg_ucns", Array("space", "endpoint", "name"), "unique_category_names", Array("space", "endpoint", "name"))

    migration.createTable("range_categories").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("data_type", Types.VARCHAR, 20, false).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 20, true).
      pk("space", "endpoint", "name")

    migration.alterTable("range_categories").
      addForeignKey("fk_racg_ucns", Array("space", "endpoint", "name"), "unique_category_names", Array("space", "endpoint", "name"))


    migration.createTable("unique_category_view_names").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      pk("space", "endpoint", "name", "view_name")

    migration.alterTable("unique_category_view_names").
      addForeignKey("fk_ucvn_evws", Array("space", "endpoint", "view_name"), "endpoint_views", Array("space", "endpoint", "name"))

    migration.createTable("prefix_category_views").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("space", "endpoint", "view_name", "name")

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_evws", Array("space", "endpoint", "view_name"), "endpoint_views", Array("space", "endpoint", "name"))

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_ucns", Array("space", "endpoint", "name", "view_name"), "unique_category_view_names", Array("space", "endpoint", "name", "view_name"))

    migration.createTable("set_category_views").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      pk("space", "endpoint",  "view_name", "name", "value")

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_evws", Array("space", "endpoint", "view_name"), "endpoint_views", Array("space", "endpoint", "name"))

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_ucns", Array("space", "endpoint", "name", "view_name"), "unique_category_view_names", Array("space", "endpoint", "name", "view_name"))

    migration.createTable("range_category_views").
      column("space", Types.BIGINT, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("data_type", Types.VARCHAR, 20, false).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 20, true).
      pk("space", "endpoint", "name", "view_name")

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_evws", Array("space", "endpoint", "view_name"), "endpoint_views", Array("space", "endpoint", "name"))

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_ucns", Array("space", "endpoint", "name", "view_name"), "unique_category_view_names", Array("space", "endpoint", "name", "view_name"))

    /*
    migration.createTable("external_http_credentials")


    migration.createTable("repair_actions")
    migration.createTable("scan_statements")

    migration.createTable("space_limits")
    migration.createTable("store_checkpoints")

    migration.createTable("unique_category_view_names")
    migration.createTable("user_item_visibility")
    */

    // Limits

    migration.createTable("limit_definitions").
      column("name", Types.VARCHAR, 50, false).
      column("description", Types.VARCHAR, 255, false).
      pk("name")

    migration.createTable("system_limits").
      column("name", Types.VARCHAR, 50, false).
      column("default_limit", Types.INTEGER, 11, false, 0).
      column("hard_limit", Types.INTEGER, 11, false, 0).
      pk("name")

    migration.createTable("space_limits").
      column("space", Types.BIGINT, false).
      column("name", Types.VARCHAR, 50, false).
      column("default_limit", Types.INTEGER, 11, false, 0).
      column("hard_limit", Types.INTEGER, 11, false, 0).
      pk("space", "name")

    migration.createTable("pair_limits").
      column("space", Types.BIGINT, false).
      column("pair", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("limit_value", Types.INTEGER, 11, false, 0).
      pk("space", "pair", "name")

    migration.alterTable("system_limits").
      addForeignKey("fk_system_limit_service_limit", "name", "limit_definitions", "name")

    migration.alterTable("space_limits").
      addForeignKey("fk_domain_limit_service_limit", "name", "limit_definitions", "name").
      addForeignKey("fk_domain_limit_space", "space", "spaces", "id")

    migration.alterTable("pair_limits").
      addForeignKey("fk_pair_limit_service_limit", "name", "limit_definitions", "name").
      addForeignKey("fk_pair_limit_pair", Array("space", "pair"), "pairs", Array("space", "name"))

    // Non-space-specific stuff

    /*

    migration.createTable("members")
    migration.createTable("sysetm_config_options")
    migration.createTable("users")
    */

    migration
  }

  def applyVerification(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    val spaceName = randomString()
    val spaceId = randomInt()

    createSpace(migration, spaceId, spaceName)
    createConfigOption(migration, spaceId)

    val upstream = randomString()
    val downstream = randomString()

    val upstreamView = randomString()
    val downstreamView = randomString()

    createEndpoint(migration, spaceId, upstream)
    createEndpointView(migration, spaceId, upstream,upstreamView)
    createEndpoint(migration, spaceId, downstream)
    createEndpointView(migration, spaceId, downstream, downstreamView)

    val pair = randomString()

    createPair(migration, spaceId, pair, upstream, downstream)
    createPairView(migration, spaceId, pair)

    val escalation = randomString()

    createEscalation(migration, spaceId, pair, escalation)
    createDiff(migration, spaceId, pair, escalation)
    createPendingDiff(migration, spaceId, pair)

    createPairReport(migration, spaceId, pair)

    val limitName = randomString()

    createLimitDefinition(migration, limitName)
    createSystemLimit(migration, limitName)
    createSpaceLimit(migration, spaceId, limitName)
    createPairLimit(migration, spaceId, pair, limitName)

    val prefixCategoryName = randomString()

    createUniqueCategoryName(migration, spaceId, upstream, prefixCategoryName)
    createUniqueCategoryViewName(migration, spaceId, upstream, upstreamView, prefixCategoryName)
    createPrefixCategory(migration, spaceId, upstream, prefixCategoryName)
    createPrefixCategoryView(migration, spaceId, upstream, upstreamView, prefixCategoryName)

    val setCategoryName = randomString()

    createUniqueCategoryName(migration, spaceId, upstream, setCategoryName)
    createUniqueCategoryViewName(migration, spaceId, upstream, upstreamView, setCategoryName)
    createSetCategory(migration, spaceId, upstream, setCategoryName)
    createSetCategoryView(migration, spaceId, upstream, upstreamView, setCategoryName)

    val rangeCategoryName = randomString()

    createUniqueCategoryName(migration, spaceId, downstream, rangeCategoryName)
    createUniqueCategoryViewName(migration, spaceId, downstream, downstreamView, rangeCategoryName)
    createRangeCategory(migration, spaceId, downstream, rangeCategoryName)
    createRangeCategoryView(migration, spaceId, downstream, downstreamView, rangeCategoryName)

    migration
  }

  def createUniqueCategoryViewName(migration:MigrationBuilder, spaceId:String, endpoint:String, view:String, name:String) {
    migration.insert("unique_category_view_names").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "view_name" -> view
    ))
  }

  def createUniqueCategoryName(migration:MigrationBuilder, spaceId:String, endpoint:String, name:String) {
    migration.insert("unique_category_names").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name
    ))
  }

  def createRangeCategory(migration:MigrationBuilder, spaceId:String, endpoint:String, name:String) {
    migration.insert("range_categories").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "data_type" -> "date",
      "lower_bound" -> "2001-10-10",
      "upper_bound" -> "2001-10-11",
      "max_granularity" -> "yearly"
    ))
  }

  def createRangeCategoryView(migration:MigrationBuilder, spaceId:String, endpoint:String, view:String, name:String) {
    migration.insert("range_category_views").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "view_name" -> view,
      "data_type" -> "date",
      "lower_bound" -> "2001-10-10",
      "upper_bound" -> "2001-10-11",
      "max_granularity" -> "yearly"
    ))
  }

  def createSetCategory(migration:MigrationBuilder, spaceId:String, endpoint:String, name:String) {
    migration.insert("set_categories").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "value" -> randomString()
    ))
  }

  def createSetCategoryView(migration:MigrationBuilder, spaceId:String, endpoint:String, view:String, name:String) {
    migration.insert("set_category_views").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "view_name" -> view,
      "value" -> randomString()
    ))
  }

  def createPrefixCategoryView(migration:MigrationBuilder, spaceId:String, endpoint:String, view:String, name:String) {
    migration.insert("prefix_category_views").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "view_name" -> view,
      "prefix_length" -> "1",
      "max_length" -> "2",
      "step" -> "1"
    ))
  }

  def createPrefixCategory(migration:MigrationBuilder, spaceId:String, endpoint:String, name:String) {
    migration.insert("prefix_categories").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name,
      "prefix_length" -> "1",
      "max_length" -> "2",
      "step" -> "1"
    ))
  }

  def createPairLimit(migration:MigrationBuilder, spaceId:String, pair:String, limit:String) {
    migration.insert("pair_limits").values(Map(
      "space" -> spaceId,
      "pair" -> pair,
      "name" -> limit,
      "limit_value" -> "100"
    ))
  }

  def createSpaceLimit(migration:MigrationBuilder, spaceId:String, limit:String) {
    migration.insert("space_limits").values(Map(
      "space" -> spaceId,
      "name" -> limit,
      "default_limit" -> "10",
      "hard_limit" -> "100"
    ))
  }

  def createSystemLimit(migration:MigrationBuilder, limit:String) {
    migration.insert("system_limits").values(Map(
      "name" -> limit,
      "default_limit" -> "10",
      "hard_limit" -> "100"
    ))
  }

  def createLimitDefinition(migration:MigrationBuilder, limit:String) {
    migration.insert("limit_definitions").values(Map(
      "name" -> limit,
      "description" -> randomString()
    ))
  }

  def createPairReport(migration:MigrationBuilder, spaceId:String, pair:String) {
    migration.insert("pair_reports").values(Map(
      "space" -> spaceId,
      "pair" -> pair,
      "name" -> randomString(),
      "report_type" -> "differences",
      "target" -> "http://example.com/bulk_diff_handler"
    ))
  }

  def createPendingDiff(migration:MigrationBuilder, spaceId:String, pair:String) {
    migration.insert("pending_diffs").values(Map(
      "space" -> spaceId,
      "pair" -> pair,
      "seq_id" -> randomInt(),
      "entity_id" -> randomString(),
      "upstream_vsn" -> randomString(),
      "downstream_vsn" -> randomString(),
      "detected_at" -> randomTimestamp(),
      "last_seen" -> randomTimestamp()
    ))
  }

  def createDiff(migration:MigrationBuilder, spaceId:String, pair:String, escalation:String) {
    migration.insert("diffs").values(Map(
      "space" -> spaceId,
      "pair" -> pair,
      "seq_id" -> randomInt(),
      "entity_id" -> randomString(),
      "upstream_vsn" -> randomString(),
      "downstream_vsn" -> randomString(),
      "detected_at" -> randomTimestamp(),
      "last_seen" -> randomTimestamp(),
      "is_match" -> "0",
      "ignored" -> "0",
      "next_escalation" -> escalation,
      "next_escalation_time" -> randomTimestamp()
    ))
  }

  def createEscalation(migration:MigrationBuilder, spaceId:String, pair:String, name:String) {
    migration.insert("escalations").values(Map(
      "space" -> spaceId,
      "pair" -> pair,
      "name" -> name,
      "action" -> randomString(),
      "action_type" -> "ignore",
      "delay" -> "10",
      "rule" -> "mismatch"
    ))
  }

  def createPairView(migration:MigrationBuilder, spaceId:String, parent:String) {
    migration.insert("pair_views").values(Map(
      "space" -> spaceId,
      "pair" -> parent,
      "name" -> randomString(),
      "scan_cron_spec" -> "0 0 * * * ?",
      "scan_cron_enabled" -> "1"
    ))
  }

  def createPair(migration:MigrationBuilder, spaceId:String, name:String, upstream:String, downstream:String) {
    migration.insert("pairs").values(Map(
      "space" -> spaceId,
      "name" -> name,
      "upstream" -> upstream,
      "downstream" -> downstream,
      "version_policy_name" -> "same",
      "matching_timeout" -> "0",
      "scan_cron_spec" -> "0 0 * * * ?",
      "scan_cron_enabled" -> "1",
      "allow_manual_scans" -> "1"
    ))
  }

  def createEndpointView(migration:MigrationBuilder, spaceId:String, endpoint:String, name:String) {
    migration.insert("endpoint_views").values(Map(
      "space" -> spaceId,
      "endpoint" -> endpoint,
      "name" -> name
    ))
  }

  def createEndpoint(migration:MigrationBuilder, spaceId:String, endpoint:String) {
    migration.insert("endpoints").values(Map(
      "space" -> spaceId,
      "name" -> endpoint,
      "scan_url" -> randomString(),
      "content_retrieval_url" -> randomString(),
      "version_generation_url" -> randomString(),
      "inbound_url" -> randomString(),
      "collation_type" -> "ascii"
    ))
  }

  def createConfigOption(migration:MigrationBuilder, spaceId:String) {
    migration.insert("config_options").values(Map(
      "space" -> spaceId,
      "opt_key" -> randomString(),
      "opt_val" -> randomString()
    ))
  }

  def createSpace(migration:MigrationBuilder, id:String, name:String) {
    migration.insert("spaces").values(Map(
      "id" -> id,
      "name" -> name,
      "config_version" -> "0"
    ))
  }

}
