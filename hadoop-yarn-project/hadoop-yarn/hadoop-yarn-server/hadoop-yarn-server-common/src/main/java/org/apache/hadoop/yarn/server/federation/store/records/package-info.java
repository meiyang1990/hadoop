// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * YARN联邦状态存储数据记录包，包含YARN联邦元数据存储体系中所有业务数据记录的定义。
 * 这些记录类型用于封装子集群注册信息、路由信息、应用状态信息等核心元数据，
 * 是YARN联邦存储层与上层逻辑之间的数据传输载体，统一了跨组件的数据交互格式。
 */
package org.apache.hadoop.yarn.server.federation.store.records;