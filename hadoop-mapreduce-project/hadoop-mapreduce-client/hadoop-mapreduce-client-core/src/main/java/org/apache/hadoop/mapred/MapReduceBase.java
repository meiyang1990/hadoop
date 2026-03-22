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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Closeable;

/** 
 * MapReduce旧API中Mapper和Reducer实现的基类
 * 
 * <p>为close()和configure()方法提供默认空实现，大多数业务应用需要根据需求重写这些方法</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapReduceBase implements Closeable, JobConfigurable {

  /** 默认空实现，不执行任何操作 */
  public void close() throws IOException {
  }

  /** 默认空实现，不执行任何操作 */
  public void configure(JobConf job) {
  }

}