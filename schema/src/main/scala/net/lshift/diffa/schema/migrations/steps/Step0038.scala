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

import net.lshift.diffa.schema.migrations.VerifiedMigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types
import scala.collection.JavaConversions._

object Step0038 extends VerifiedMigrationStep {

  def versionId = 38

  def name = "De-hibernatize category storage"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    // Create the parent table that needs to get inserted into to make sure that each child category has a unique name

    migration.createTable("unique_category_names").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      pk("domain", "endpoint", "name")

    migration.alterTable("unique_category_names").
      addForeignKey("fk_ucns_edpt", Array("domain", "endpoint"), "endpoint", Array("domain", "name"))

    migration.createTable("unique_category_view_names").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      pk("domain", "endpoint", "name", "view_name")

    migration.alterTable("unique_category_view_names").
      addForeignKey("fk_ucvn_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    // Create the new table for prefix categories

    migration.createTable("prefix_categories").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("domain", "endpoint", "name")

    migration.alterTable("prefix_categories").
      addForeignKey("fk_pfcg_ucns", Array("domain", "endpoint", "name"), "unique_category_names", Array("domain", "endpoint", "name"))

    migration.createTable("prefix_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("domain", "endpoint", "view_name", "name")

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_view_names", Array("domain", "endpoint", "name", "view_name"))

    // Create a parent record for all to-be-migrated prefix categories on endpoints proper

    migration.copyTableContents("category_descriptor", "unique_category_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      whereSource(Map("constraint_type" -> "prefix")).
      notNull(Seq("domain", "name", "id"))

    // Create a parent record for all to-be-migrated prefix categories on endpoint views

    migration.copyTableContents("category_descriptor", "unique_category_view_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint")).
      whereSource(Map("constraint_type" -> "prefix")).
      notNull(Seq("domain", "category_name", "endpoint"))

    // Migrate all prefix categories on endpoints proper

    migration.copyTableContents("category_descriptor", "prefix_categories",
      Seq("prefix_length", "max_length", "step"),
      Seq("prefix_length", "max_length", "step", "domain", "name", "endpoint")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      whereSource(Map("constraint_type" -> "prefix"))

    // Migrate all prefix categories on endpoint views

    migration.copyTableContents("category_descriptor", "prefix_category_views",
      Seq("prefix_length", "max_length", "step"),
      Seq("prefix_length", "max_length", "step", "domain", "name", "endpoint", "view_name")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint", "name")).
      whereSource(Map("constraint_type" -> "prefix"))

    // Nuke old prefix descriptors

    migration.dropTable("prefix_category_descriptor")

    // Create the new table for set categories

    migration.createTable("set_categories").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      pk("domain", "endpoint", "name", "value")

    migration.alterTable("set_categories").
      addForeignKey("fk_stcg_ucns", Array("domain", "endpoint", "name"), "unique_category_names", Array("domain", "endpoint", "name"))

    migration.createTable("set_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      pk("domain", "endpoint",  "view_name", "name", "value")

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_view_names", Array("domain", "endpoint", "name", "view_name"))

    // Create a parent record for all to-be-migrated set categories on endpoints proper

    migration.copyTableContents("category_descriptor", "unique_category_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      whereSource(Map("constraint_type" -> "set"))

    // Migrate all set categories on endpoints proper

    migration.copyTableContents("category_descriptor", "set_categories",
      Seq("domain", "name", "endpoint", "value")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      join("set_constraint_values", "value_id", "category_id", Seq("value_name")).
      whereSource(Map("constraint_type" -> "set"))

    // Create a parent record for all to-be-migrated set categories on endpoint views

    migration.copyTableContents("category_descriptor", "unique_category_view_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint")).
      whereSource(Map("constraint_type" -> "set"))

    // Migrate all set categories on endpoint views

    migration.copyTableContents("category_descriptor", "set_category_views",
      Seq("domain", "name", "endpoint", "view_name", "value")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint", "name")).
      join("set_constraint_values", "value_id", "category_id", Seq("value_name")).
      whereSource(Map("constraint_type" -> "set"))

    // Nuke old set descriptors and their associated values

    migration.dropTable("set_category_descriptor")
    migration.dropTable("set_constraint_values")

    migration.createTable("range_categories").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("data_type", Types.VARCHAR, 20, false).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 20, true).
      pk("domain", "endpoint", "name")

    migration.alterTable("range_categories").
      addForeignKey("fk_racg_ucns", Array("domain", "endpoint", "name"), "unique_category_names", Array("domain", "endpoint", "name"))

    migration.createTable("range_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("data_type", Types.VARCHAR, 20, false).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 20, true).
      pk("domain", "endpoint", "name", "view_name")

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_view_names", Array("domain", "endpoint", "name", "view_name"))

    // Create a parent record for all to-be-migrated range categories on endpoints proper

    migration.copyTableContents("category_descriptor", "unique_category_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      whereSource(Map("constraint_type" -> "range"))

    // Migrate all range categories on endpoints proper

    migration.copyTableContents("category_descriptor", "range_categories",
      Seq("domain", "name", "endpoint", "data_type","lower_bound", "upper_bound", "max_granularity")).
      join("endpoint_categories", "category_descriptor_id", "category_id", Seq("domain", "name", "id")).
      join("range_category_descriptor", "id", "category_id", Seq("data_type","lower_bound", "upper_bound", "max_granularity")).
      whereSource(Map("constraint_type" -> "range"))

    // Create a parent record for all to-be-migrated range categories on endpoint views

    migration.copyTableContents("category_descriptor", "unique_category_view_names",
      Seq("domain", "name", "endpoint")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint")).
      whereSource(Map("constraint_type" -> "range"))

    // Migrate all range categories on endpoint views

    migration.copyTableContents("category_descriptor", "range_category_views",
      Seq("domain", "name", "endpoint", "view_name", "data_type","lower_bound", "upper_bound", "max_granularity")).
      join("endpoint_views_categories", "category_descriptor_id", "category_id", Seq("domain", "category_name", "endpoint", "name")).
      join("range_category_descriptor", "id", "category_id", Seq("data_type","lower_bound", "upper_bound", "max_granularity")).
      whereSource(Map("constraint_type" -> "range"))

    // Nuke old range descriptors

    migration.dropTable("range_category_descriptor")

    // Blast away the n:m tables that linked the various category tables with the respective endpoint and endpoint views tables

    migration.dropTable("endpoint_categories")
    migration.dropTable("endpoint_views_categories")

    migration
  }

  def applyVerification(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    val domain = randomString()
    val upstreamEndpoint = randomString()
    val upstreamEndpointView = randomString()
    val downstreamEndpoint = randomString()
    val downstreamEndpointView = randomString()

    // 1. Set up the domain with an upstream and a downstream endpoint

    migration.insert("domains").values(Map(
      "name"  -> domain
    ))

    migration.insert("endpoint").values(Map(
      "domain"  -> domain,
      "name"    -> upstreamEndpoint
    ))

    migration.insert("endpoint").values(Map(
      "domain"  -> domain,
      "name"    -> downstreamEndpoint
    ))

    migration.insert("endpoint_views").values(Map(
      "domain"    -> domain,
      "endpoint"  -> upstreamEndpoint,
      "name"      -> upstreamEndpointView
    ))

    migration.insert("endpoint_views").values(Map(
      "domain"    -> domain,
      "endpoint"  -> downstreamEndpoint,
      "name"      -> downstreamEndpointView
    ))

    // 2. Add a range category to each of the upstream and downstream parents

    migration.insert("unique_category_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> upstreamEndpoint,
      "name"      -> "some-date-based-category"
    ))

    migration.insert("unique_category_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> downstreamEndpoint,
      "name"      -> "some-date-based-category"
    ))

    migration.insert("range_categories").values(Map(
      "domain"          -> domain,
      "endpoint"        -> upstreamEndpoint,
      "name"            -> "some-date-based-category",
      "data_type"       -> "date",
      "lower_bound"     -> "1999-10-10",
      "upper_bound"     -> "1999-10-11",
      "max_granularity" -> "daily"
    ))

    migration.insert("range_categories").values(Map(
      "domain"          -> domain,
      "endpoint"        -> downstreamEndpoint,
      "name"            -> "some-date-based-category",
      "data_type"       -> "date",
      "lower_bound"     -> "1999-10-10",
      "upper_bound"     -> "1999-10-11",
      "max_granularity" -> "daily"
    ))

    // 3. Add a range category to each of the respective view

    migration.insert("unique_category_view_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> upstreamEndpoint,
      "name"      -> "some-date-based-category",
      "view_name" -> upstreamEndpointView
    ))

    migration.insert("unique_category_view_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> downstreamEndpoint,
      "name"      -> "some-date-based-category",
      "view_name" -> downstreamEndpointView
    ))

    migration.insert("range_category_views").values(Map(
      "domain"          -> domain,
      "endpoint"        -> upstreamEndpoint,
      "name"            -> "some-date-based-category",
      "view_name"       -> upstreamEndpointView,
      "data_type"       -> "date",
      "lower_bound"     -> "1999-10-10",
      "upper_bound"     -> "1999-10-11",
      "max_granularity" -> "daily"
    ))

    migration.insert("range_category_views").values(Map(
      "domain"          -> domain,
      "endpoint"        -> downstreamEndpoint,
      "name"            -> "some-date-based-category",
      "view_name"       -> downstreamEndpointView,
      "data_type"       -> "date",
      "lower_bound"     -> "1999-10-10",
      "upper_bound"     -> "1999-10-11",
      "max_granularity" -> "daily"
    ))

    // Make sure that set and prefix categories can also still be migrated (each time just on one of the endpoints, this will probably suffice)

    // 4. Put a prefix category with a view onto the upstream

    migration.insert("unique_category_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> upstreamEndpoint,
      "name"      -> "some-prefix-based-category"
    ))

    migration.insert("prefix_categories").values(Map(
      "domain"          -> domain,
      "endpoint"        -> upstreamEndpoint,
      "name"            -> "some-prefix-based-category",
      "prefix_length"   -> "1",
      "max_length"      -> "2",
      "step"            -> "1"
    ))

    migration.insert("unique_category_view_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> upstreamEndpoint,
      "name"      -> "some-prefix-based-category",
      "view_name" -> upstreamEndpointView
    ))

    migration.insert("prefix_category_views").values(Map(
      "domain"          -> domain,
      "endpoint"        -> upstreamEndpoint,
      "name"            -> "some-prefix-based-category",
      "view_name"       -> upstreamEndpointView,
      "prefix_length"   -> "1",
      "max_length"      -> "2",
      "step"            -> "1"
    ))

    // 5. Put a set category with a view onto the downstream

    migration.insert("unique_category_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> downstreamEndpoint,
      "name"      -> "some-set-based-category"
    ))

    migration.insert("set_categories").values(Map(
      "domain"          -> domain,
      "endpoint"        -> downstreamEndpoint,
      "name"            -> "some-set-based-category",
      "value"           -> randomString()
    ))

    migration.insert("unique_category_view_names").values(Map(
      "domain"    -> domain,
      "endpoint"  -> downstreamEndpoint,
      "name"      -> "some-set-based-category",
      "view_name" -> downstreamEndpointView
    ))

    migration.insert("set_category_views").values(Map(
      "domain"          -> domain,
      "endpoint"        -> downstreamEndpoint,
      "name"            -> "some-set-based-category",
      "view_name"       -> downstreamEndpointView,
      "value"           -> randomString()
    ))

    migration
  }
}
