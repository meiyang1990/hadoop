// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

/**
 * YARN NodeManager 出站网络带宽资源处理接口，定义容器出站带宽资源的隔离与管控能力
 * 属于Linux容器资源管理模块，基于cgroup等机制实现网络带宽限流
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 出站网络带宽资源处理器接口，继承通用资源处理器接口
 * 负责对容器的出站网络带宽进行资源隔离和流量控制，保障集群节点网络资源公平分配
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface OutboundBandwidthResourceHandler extends ResourceHandler {
}