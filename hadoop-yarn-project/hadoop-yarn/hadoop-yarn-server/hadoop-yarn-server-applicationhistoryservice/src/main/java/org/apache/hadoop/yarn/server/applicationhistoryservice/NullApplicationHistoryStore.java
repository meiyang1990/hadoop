// 这个文件已经全部加上中文注释
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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

/**
 * 空实现的应用历史存储，所有操作都是空操作。
 * 
 * 使用此实现时，历史数据不会被持久化，读取操作返回空结果。
 */
@Unstable
@Private
public class NullApplicationHistoryStore extends AbstractService implements
    ApplicationHistoryStore {

  public NullApplicationHistoryStore() {
    super(NullApplicationHistoryStore.class.getName());
  }

  @Override
  public void applicationStarted(ApplicationStartData appStart)
      throws IOException {
  }

  @Override
  public void applicationFinished(ApplicationFinishData appFinish)
      throws IOException {
  }

  @Override
  public void applicationAttemptStarted(
      ApplicationAttemptStartData appAttemptStart) throws IOException {
  }

  @Override
  public void applicationAttemptFinished(
      ApplicationAttemptFinishData appAttemptFinish) throws IOException {
  }

  @Override
  public void containerStarted(ContainerStartData containerStart)
      throws IOException {
  }

  @Override
  public void containerFinished(ContainerFinishData containerFinish)
      throws IOException {
  }

  @Override
  public ApplicationHistoryData getApplication(ApplicationId appId)
      throws IOException {
    return null;
  }

  @Override
  public Map<ApplicationId, ApplicationHistoryData> getAllApplications()
      throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getApplicationAttempts(ApplicationId appId) throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException {
    return null;
  }

  @Override
  public ContainerHistoryData getContainer(ContainerId containerId)
      throws IOException {
    return null;
  }

  @Override
  public ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException {
    return null;
  }

  @Override
  public Map<ContainerId, ContainerHistoryData> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    return Collections.emptyMap();
  }

}