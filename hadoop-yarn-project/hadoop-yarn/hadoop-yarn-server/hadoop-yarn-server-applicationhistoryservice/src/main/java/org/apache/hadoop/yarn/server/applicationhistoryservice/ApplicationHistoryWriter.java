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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

/**
 * 应用历史数据写入器接口，用于写入应用、应用尝试和容器的生命周期数据。
 */
@Private
@Unstable
public interface ApplicationHistoryWriter {

  /**
   * 写入应用启动时的信息。
   * 
   * @param appStart 应用启动数据
   * @throws IOException
   */
  void applicationStarted(ApplicationStartData appStart) throws IOException;

  /**
   * 写入应用结束时的信息。
   * 
   * @param appFinish 应用结束数据
   * @throws IOException
   */
  void applicationFinished(ApplicationFinishData appFinish) throws IOException;

  /**
   * 写入应用尝试启动时的信息。
   * 
   * @param appAttemptStart 应用尝试启动数据
   * @throws IOException
   */
  void applicationAttemptStarted(ApplicationAttemptStartData appAttemptStart)
      throws IOException;

  /**
   * 写入应用尝试结束时的信息。
   * 
   * @param appAttemptFinish 应用尝试结束数据
   * @throws IOException
   */
  void
      applicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish)
          throws IOException;

  /**
   * 写入容器启动时的信息。
   * 
   * @param containerStart 容器启动数据
   * @throws IOException
   */
  void containerStarted(ContainerStartData containerStart) throws IOException;

  /**
   * 写入容器结束时的信息。
   * 
   * @param containerFinish 容器结束数据
   * @throws IOException
   */
  void containerFinished(ContainerFinishData containerFinish)
      throws IOException;

}
