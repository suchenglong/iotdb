/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.multileader.service;

import org.apache.iotdb.commons.StepTracker;
import org.apache.iotdb.consensus.multileader.MultiLeaderConsensus;
import org.apache.iotdb.consensus.multileader.thrift.MultiLeaderConsensusIService;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogReq;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogRes;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MultiLeaderRPCServiceProcessor implements MultiLeaderConsensusIService.AsyncIface {

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderRPCServiceProcessor.class);

  private final MultiLeaderConsensus consensus;

  public MultiLeaderRPCServiceProcessor(MultiLeaderConsensus consensus) {
    this.consensus = consensus;
  }

  @Override
  public void syncLog(TSyncLogReq req, AsyncMethodCallback<TSyncLogRes> resultHandler)
      throws TException {
    long startTime = System.nanoTime();
    try {
      //      ConsensusGroupId groupId =
      //          ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      //      MultiLeaderServerImpl impl = consensus.getImpl(groupId);
      //      if (impl == null) {
      //        String message =
      //            String.format(
      //                "Unexpected consensusGroupId %s for TSyncLogReq which size is %s",
      //                groupId, req.getBatches().size());
      //        logger.error(message);
      //        TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      //        status.setMessage(message);
      //        resultHandler.onComplete(new TSyncLogRes(Collections.singletonList(status)));
      //        return;
      //      }
      //      List<TSStatus> statuses = new ArrayList<>();
      //      // We use synchronized to ensure atomicity of executing multiple logs
      //      synchronized (impl.getStateMachine()) {
      //        for (TLogBatch batch : req.getBatches()) {
      //          statuses.add(
      //              impl.getStateMachine()
      //                  .write(
      //                      impl.buildIndexedConsensusRequestForRemoteRequest(
      //                          new ByteBufferConsensusRequest(batch.data))));
      //        }
      //      }
      //      logger.debug("Execute TSyncLogReq for {} with result {}", req.consensusGroupId,
      // statuses);
      resultHandler.onComplete(new TSyncLogRes(new ArrayList<>()));
    } catch (Exception e) {
      resultHandler.onError(e);
    } finally {
      StepTracker.trace("ProcessSyncLog", 100, startTime, System.nanoTime());
    }
  }

  public void handleClientExit() {}
}