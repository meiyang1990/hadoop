// 这个文件已经全部加上中文注释
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * HDFS数据传输协议包，包含HDFS客户端与DataNode之间、DataNode之间进行数据块传输
 * 所使用的协议定义与核心数据结构，支撑HDFS的数据读写、块复制、数据恢复等核心流程。
 */
@InterfaceStability.Evolving
package org.apache.hadoop.hdfs.protocol.datatransfer;
import org.apache.hadoop.classification.InterfaceStability;