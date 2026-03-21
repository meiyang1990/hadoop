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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyRackFaultTolerant;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaLruTracker;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.apache.hadoop.hdfs.server.namenode.fgl.GlobalFSNamesystemLock;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;

import java.util.concurrent.TimeUnit;

/** 
 * HDFS模块所有配置项键和默认值的常量容器，统一管理HDFS服务端和客户端的配置定义。
 */
@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  public static final String  DFS_BLOCK_SIZE_KEY =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
  public static final long    DFS_BLOCK_SIZE_DEFAULT =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  public static final String  DFS_REPLICATION_KEY =
      HdfsClientConfigKeys.DFS_REPLICATION_KEY;
  public static final short   DFS_REPLICATION_DEFAULT =
      HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;

  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_KEY =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_KEY;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_DEFAULT =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT;
  public static final String  DFS_CHECKSUM_TYPE_KEY = HdfsClientConfigKeys
      .DFS_CHECKSUM_TYPE_KEY;
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT =
      HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
  @Deprecated
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY;
  @Deprecated
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;
  public static final String  DFS_WEBHDFS_NETTY_LOW_WATERMARK =
      "dfs.webhdfs.netty.low.watermark";
  public static final int  DFS_WEBHDFS_NETTY_LOW_WATERMARK_DEFAULT = 32768;
  public static final String  DFS_WEBHDFS_NETTY_HIGH_WATERMARK =
      "dfs.webhdfs.netty.high.watermark";
  public static final int  DFS_WEBHDFS_NETTY_HIGH_WATERMARK_DEFAULT = 65535;
  public static final String  DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_KEY =
      "dfs.webhdfs.ugi.expire.after.access";
  public static final int     DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_DEFAULT =
      10*60*1000; //10 minutes
  public static final String DFS_WEBHDFS_USE_IPC_CALLQ =
      "dfs.webhdfs.use.ipc.callq";
  public static final boolean DFS_WEBHDFS_USE_IPC_CALLQ_DEFAULT = true;

  // HA related configuration
  // 高可用相关配置
  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final long    DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT = 50;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT = "localhost:50100";
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT = "0.0.0.0:50105";
  public static final String  DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.backup.dnrpc-address";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS =
      HdfsClientConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT = "0.0.0.0:50200";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST = "dfs.provided.aliasmap.inmemory.rpc.bind-host";

  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR = "dfs.provided.aliasmap.inmemory.leveldb.dir";
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE = "dfs.provided.aliasmap.inmemory.batch-size";
  public static final int DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE_DEFAULT = 500;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED = "dfs.provided.aliasmap.inmemory.enabled";
  public static final boolean DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED_DEFAULT = false;
  public static final String DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG = "dfs.provided.aliasmap.inmemory.server.log";
  public static final boolean DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT = false;

  public static final String  DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY;
  public static final long    DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT =
      100 * 1024*1024;
  public static final String  DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY
      = "dfs.datanode.balance.max.concurrent.moves";
  public static final int
      DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT = 100;
  public static final String DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.transfer.bandwidthPerSec";
  public static final long DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  public static final String DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.write.bandwidthPerSec";
  // A value of zero indicates no limit
  public static final long DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT = 0;
  public static final String DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.data.read.bandwidthPerSec";
  // A value of zero indicates no limit
  public static final long DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT = 0;
  public static final String DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.ec.reconstruct.read.bandwidthPerSec";
  public static final long DFS_DATANODE_EC_RECONSTRUCT_READ_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  public static final String DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.ec.reconstruct.write.bandwidthPerSec";
  public static final long DFS_DATANODE_EC_RECONSTRUCT_WRITE_BANDWIDTHPERSEC_DEFAULT =
      0; // A value of zero indicates no limit
  @Deprecated
  public static final String  DFS_DATANODE_READAHEAD_BYTES_KEY =
      HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY;
  @Deprecated
  public static final long    DFS_DATANODE_READAHEAD_BYTES_DEFAULT =
      HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT;
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY = "dfs.datanode.drop.cache.behind.writes";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_SYNC_BEHIND_WRITES_KEY = "dfs.datanode.sync.behind.writes";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT = false;
  public static final String  DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_KEY = "dfs.datanode.sync.behind.writes.in.background";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_DEFAULT = false;
  public static final String  DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY = "dfs.datanode.drop.cache.behind.reads";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT = false;
  public static final String  DFS_DATANODE_USE_DN_HOSTNAME = "dfs.datanode.use.datanode.hostname";
  public static final boolean DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT = false;
  public static final String  DFS_DATANODE_MAX_LOCKED_MEMORY_KEY = "dfs.datanode.max.locked.memory";
  public static final long    DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT = 0;
  public static final String  DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY = "dfs.datanode.fsdatasetcache.max.threads.per.volume";
  public static final int     DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT = 4;
  public static final String  DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_KEY =
      "dfs.datanode.fsdatasetasyncdisk.max.threads.per.volume";
  public static final int     DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_DEFAULT = 4;
  public static final String  DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC = "dfs.datanode.lazywriter.interval.sec";
  public static final int     DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC = 60;
  public static final String  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_KEY = "dfs.datanode.ram.disk.replica.tracker";
  public static final Class<RamDiskReplicaLruTracker>  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT = RamDiskReplicaLruTracker.class;
  public static final String  DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY = "dfs.datanode.network.counts.cache.max.size";
  public static final int     DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;
  public static final String DFS_DATANODE_NON_LOCAL_LAZY_PERSIST =
      "dfs.datanode.non.local.lazy.persist";
  public static final boolean DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT =
      false;
  public static final String DFS_DATANODE_FIXED_VOLUME_SIZE_KEY =
      "dfs.datanode.fixed.volume.size";
  public static final boolean DFS_DATANODE_FIXED_VOLUME_SIZE_DEFAULT = false;
  public static final String  DFS_DATANODE_REPLICA_CACHE_ROOT_DIR_KEY =
      "dfs.datanode.replica.cache.root.dir";
  public static final String DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_KEY =
      "dfs.datanode.replica.cache.expiry.time";
  public static final long DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_DEFAULT =
      300000;

  // This setting is for testing/internal use only.
  // 仅用于测试/内部使用，控制是否删除重复副本
  public static final String  DFS_DATANODE_DUPLICATE_REPLICA_DELETION = "dfs.datanode.duplicate.replica.deletion";
  public static final boolean DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT = true;

  public static final String DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS =
      "dfs.datanode.cached-dfsused.check.interval.ms";
  public static final long DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_DEFAULT_MS =
      600000;

  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT =
      "dfs.namenode.path.based.cache.block.map.allocation.percent";
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  // NameNode CRM锁检测相关配置
  public static final String DFS_NAMENODE_CRM_CHECKLOCKTIME_ENABLE =
      "dfs.namenode.crm.checklocktime.enable";
  public static final boolean DFS_NAMENODE_CRM_CHECKLOCKTIME_DEFAULT = false;

  public static final String DFS_NAMENODE_CRM_MAXLOCKTIME_MS =
      "dfs.namenode.crm.maxlocktime.ms";
  public static final long DFS_NAMENODE_CRM_MAXLOCKTIME_MS_DEFAULT = 1000;

  public static final String DFS_NAMENODE_CRM_SLEEP_TIME_MS =
      "dfs.namenode.crm.sleeptime.ms";
  public static final long DFS_NAMENODE_CRM_SLEEP_TIME_MS_DEFAULT = 300;

  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_BIND_HOST_KEY = "dfs.namenode.http-bind-host";
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_RPC_BIND_HOST_KEY = "dfs.namenode.rpc-bind-host";
  public static final String  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.servicerpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY = "dfs.namenode.servicerpc-bind-host";
  public static final String  DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY =
      "dfs.namenode.lifeline.rpc-address";
  public static final String