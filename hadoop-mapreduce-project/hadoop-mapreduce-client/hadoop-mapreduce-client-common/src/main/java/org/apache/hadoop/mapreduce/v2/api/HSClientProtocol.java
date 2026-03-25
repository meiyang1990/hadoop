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

package org.apache.hadoop.mapreduce.v2.api;

/**
 * 文件说明：历史服务器客户端协议接口，定义客户端与MapReduce历史服务器之间的通信契约
 * 该接口继承自通用MR客户端协议，用于专门标识历史服务器服务的通信接口，
 * 供Hadoop客户端调用历史服务器服务，查询已完成MapReduce作业的历史信息。
 */
public interface HSClientProtocol extends MRClientProtocol {
}