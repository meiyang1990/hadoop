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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;

/**
 * 应用历史数据存储接口，继承了读取和写入接口。
 * 
 * 作为 {@link Service} 的实现，可利用服务生命周期来初始化和清理存储。
 * 用户通过 {@link ApplicationHistoryReader} 和 {@link ApplicationHistoryWriter} 接口访问存储。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ApplicationHistoryStore extends Service,
    ApplicationHistoryReader, ApplicationHistoryWriter {
}
