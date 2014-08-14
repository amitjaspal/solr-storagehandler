/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.solr;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

/*
 * SolrMetaHook module is responsible for making sure that SOLR backed
 * data store is in sync with the Hive meta-store. For version 1.0 we
 * don't plan to create or delete SOLR collections, but this api can
 * be useful going forward.
 */
public class SolrMetaHook implements HiveMetaHook{

  @Override
  public void preCreateTable(Table table) throws MetaException {

  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {

  }

  @Override
  public void preDropTable(Table table) throws MetaException {

  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {

  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {

  }
}
