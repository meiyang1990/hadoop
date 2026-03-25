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
 * @file package-info.java
 * @brief 本包提供NodeManager向时间线服务V2版本(ATSv2)上报各类事件的相关实现
 * 
 * 核心职责：实现NodeManager生命周期事件、容器事件的采集与上报，为YARN Timeline Service
 * 提供节点侧的事件数据源，支撑应用运行指标与生命周期的可视化监控。
 */
/**
 * Package org.apache.hadoop.yarn.server.nodemanager.timelineservice contains
 * classes related to publishing container events and other NM lifecycle events
 * to ATSv2.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;