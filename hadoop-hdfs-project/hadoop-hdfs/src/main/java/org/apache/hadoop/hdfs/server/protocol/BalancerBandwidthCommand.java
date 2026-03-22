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
package org.apache.hadoop.hdfs.server.protocol;

/*
 * A system administrator can tune the balancer bandwidth parameter
 * (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
 * "dfsadmin -setBalanacerBandwidth newbandwidth".
 * This class is to define the command which sends the new bandwidth value to
 * each datanode.
 */

/**
 * 数据节点均衡带宽调整命令，用于NameNode向DataNode下发动态调整均衡带宽的指令
 * 该命令属于HDFS集群平衡协议的核心数据结构，承载新的均衡带宽值，支持管理员动态修改
 * 每个DataNode在块平衡操作中可使用的最大网络带宽，无需重启节点即可生效
 */
public class BalancerBandwidthCommand extends DatanodeCommand {
  private final static long BBC_DEFAULTBANDWIDTH = 0L;

  private final long bandwidth;

  /**
   * 无参构造函数，使用默认带宽值0初始化命令
   */
  BalancerBandwidthCommand() {
    this(BBC_DEFAULTBANDWIDTH);
  }

  /**
   * 全参构造函数，使用指定带宽值创建均衡带宽调整命令
   *
   * @param bandwidth 新的均衡带宽值，单位为字节/秒
   */
  public BalancerBandwidthCommand(long bandwidth) {
    super(DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE);
    this.bandwidth = bandwidth;
  }

  /**
   * 获取命令中携带的目标均衡带宽值
   *
   * @return 目标均衡带宽值，单位为字节/秒
   */
  public long getBalancerBandwidthValue() {
    return this.bandwidth;
  }
}