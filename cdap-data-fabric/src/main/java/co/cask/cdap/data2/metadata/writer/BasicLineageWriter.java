/*
 * Copyright © 2015-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageTable;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageDataset;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Basic implementation of {@link LineageWriter} and {@link FieldLineageWriter}.
 * Implementation of LineageWriter write to the {@link LineageTable} where as
 * implementation of FieldLineageWriter writes to the {@link FieldLineageDataset} directly.
 */
public class BasicLineageWriter implements LineageWriter, FieldLineageWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLineageWriter.class);

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final TransactionRunner transactionRunner;

  @VisibleForTesting
  @Inject
  public BasicLineageWriter(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                            TransactionRunner transactionRunner) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetId, AccessType accessType,
                        @Nullable NamespacedEntityId namespacedEntityId) {
    // Don't record lineage for the lineage dataset itself, otherwise there would be infinite recursion
    if (StoreDefinition.LineageStore.DATASET_LINEAGE_TABLE.equals(datasetId)
      || StoreDefinition.LineageStore.PROGRAM_LINEAGE_TABLE.equals(datasetId)) {
      return;
    }

    long accessTime = System.currentTimeMillis();
    LOG.trace("Writing access for run {}, dataset {}, accessType {}, accessTime = {}",
              run, datasetId, accessType, accessTime);

    TransactionRunners.run(transactionRunner, context -> {
      LineageTable
        .getLineageDataset(context)
        .addAccess(run, datasetId, accessType, accessTime);
    });
  }

  /**
   * Returns the {@link DatasetId} of the field lineage dataset. This method should only be overridden in unit-test.
   */
  @VisibleForTesting
  protected DatasetId getFieldLineageDatasetId() {
    return FieldLineageDataset.FIELD_LINEAGE_DATASET_ID;
  }

  @Override
  public void write(ProgramRunId programRunId, FieldLineageInfo info) {
    Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
                                                                                           getFieldLineageDatasetId());

      fieldLineageDataset.addFieldLineageInfo(programRunId, info);
    });
  }
}
