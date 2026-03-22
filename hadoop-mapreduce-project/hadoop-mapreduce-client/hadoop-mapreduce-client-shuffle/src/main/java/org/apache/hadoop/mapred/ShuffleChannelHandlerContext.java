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

package org.apache.hadoop.mapred;

import io.netty.channel.group.ChannelGroup;

import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.util.Shell;

import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_MAX_SHUFFLE_CONNECTIONS;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MANAGE_OS_CACHE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_READAHEAD_BYTES;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.MAX_SHUFFLE_CONNECTIONS;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MANAGE_OS_CACHE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MAX_SESSION_OPEN_FILES;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_READAHEAD_BYTES;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_TRANSFERTO_ALLOWED;
import static org.apache.hadoop.mapred.ShuffleHandler.SUFFLE_SSL_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.mapred.ShuffleHandler.WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED;

/**
 * Shuffle阶段Netty服务端上下文容器，保存ShuffleHandler处理请求所需的所有共享配置与状态资源。
 * 为每个Shuffle通道处理提供统一的上下文访问入口，聚合配置、缓存、 metrics、连接管理等核心资源。
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class ShuffleChannelHandlerContext {

  public final Configuration conf;
  public final JobTokenSecretManager secretManager;
  public final Map<String, String> userRsrc;
  public final LoadingCache<ShuffleHandler.AttemptPathIdentifier,
      ShuffleHandler.AttemptPathInfo> pathCache;
  public final IndexCache indexCache;
  public final ShuffleHandler.ShuffleMetrics metrics;
  public final ChannelGroup allChannels;


  public final boolean connectionKeepAliveEnabled;
  public final int sslFileBufferSize;
  public final int connectionKeepAliveTimeOut;
  public final int mapOutputMetaInfoCacheSize;

  public final AtomicInteger activeConnections = new AtomicInteger();

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile.
   */
  public final boolean manageOsCache;
  public final int readaheadLength;
  public final int maxShuffleConnections;
  public final int shuffleBufferSize;
  public final boolean shuffleTransferToAllowed;
  public final int maxSessionOpenFiles;
  public final ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  public int port = -1;

  /**
   * 构造Shuffle通道上下文，从配置中加载所有Shuffle服务参数并初始化共享资源。
   * @param conf Hadoop配置对象
   * @param userRsrc 用户资源映射表
   * @param secretManager Job令牌密钥管理器，用于认证请求
   * @param patCache 尝试任务输出路径缓存，缓存map输出文件路径信息
   * @param indexCache  map输出索引缓存，加速索引数据读取
   * @param metrics Shuffle服务指标统计对象
   * @param allChannels Netty全通道组，管理所有活跃连接
   */
  public ShuffleChannelHandlerContext(Configuration conf,
                                      Map<String, String> userRsrc,
                                      JobTokenSecretManager secretManager,
                                      LoadingCache<ShuffleHandler.AttemptPathIdentifier,
                                          ShuffleHandler.AttemptPathInfo> patCache,
                                      IndexCache indexCache,
                                      ShuffleHandler.ShuffleMetrics metrics,
                                      ChannelGroup allChannels) {
    this.conf = conf;
    this.userRsrc = userRsrc;
    this.secretManager = secretManager;
    this.pathCache = patCache;
    this.indexCache = indexCache;
    this.metrics = metrics;
    this.allChannels = allChannels;

    // 从配置加载SSL文件缓冲区大小
    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
        DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
    // 从配置加载连接保活开关
    connectionKeepAliveEnabled =
        conf.getBoolean(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED);
    // 从配置加载连接保活超时时间，最小为1秒
    connectionKeepAliveTimeOut =
        Math.max(1, conf.getInt(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT));
    // 从配置加载map输出元信息缓存大小，最小为1
    mapOutputMetaInfoCacheSize =
        Math.max(1, conf.getInt(SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE,
            DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE));

    // 从配置加载是否管理OS页缓存开关
    manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
        DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    // 从配置加载预读字节数
    readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
        DEFAULT_SHUFFLE_READAHEAD_BYTES);

    // 从配置加载最大Shuffle连接数
    maxShuffleConnections = conf.getInt(MAX_SHUFFLE_CONNECTIONS,
        DEFAULT_MAX_SHUFFLE_CONNECTIONS);

    // 从配置加载Shuffle缓冲区大小
    shuffleBufferSize = conf.getInt(SHUFFLE_BUFFER_SIZE,
        DEFAULT_SHUFFLE_BUFFER_SIZE);

    // 从配置加载是否允许零拷贝transferTo传输，Windows平台使用不同默认值
    shuffleTransferToAllowed = conf.getBoolean(SHUFFLE_TRANSFERTO_ALLOWED,
        (Shell.WINDOWS)?WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED:
            DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED);

    // 从配置加载单个会话最大打开文件数
    maxSessionOpenFiles = conf.getInt(SHUFFLE_MAX_SESSION_OPEN_FILES,
        DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);
  }

  /**
   * 设置Shuffle服务监听端口，用于上下文记录端口信息。
   * @param port 监听端口号
   */
  void setPort(int port) {
    this.port = port;
  }
}