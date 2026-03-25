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

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import javax.annotation.Nonnull;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.proto.ShuffleHandlerRecoveryProtos.JobShuffleInfoProto;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * MapReduce Shuffle服务实现，作为YARN NodeManager的辅助服务，
 * 负责为Reduce任务提供Map输出数据的拉取服务，基于Netty实现高性能HTTP数据传输。
 * 核心职责：管理作业token认证、维护Map输出路径缓存、提供数据拉取接口、支持NM重启后状态恢复。
 */
public class ShuffleHandler extends AuxiliaryService {

  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(ShuffleHandler.class);
  public static final org.slf4j.Logger AUDITLOG =
      LoggerFactory.getLogger(ShuffleHandler.class.getName()+".audit");
  public static final String SHUFFLE_MANAGE_OS_CACHE = "mapreduce.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "mapreduce.shuffle.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  public static final String MAX_WEIGHT =
      "mapreduce.shuffle.pathcache.max-weight";
  public static final int DEFAULT_MAX_WEIGHT = 10 * 1024 * 1024;

  public static final String EXPIRE_AFTER_ACCESS_MINUTES =
      "mapreduce.shuffle.pathcache.expire-after-access-minutes";
  public static final int DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES = 5;

  public static final String CONCURRENCY_LEVEL =
      "mapreduce.shuffle.pathcache.concurrency-level";
  public static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  
  // pattern to identify errors related to the client closing the socket early
  // idea borrowed from Netty SslHandler
  public static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
      "^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$",
      Pattern.CASE_INSENSITIVE);

  private static final String STATE_DB_NAME = "mapreduce_shuffle_state";
  private static final String STATE_DB_SCHEMA_VERSION_KEY = "shuffle-schema-version";
  protected static final Version CURRENT_VERSION_INFO = 
      Version.newInstance(1, 0);

  private static final String DATA_FILE_NAME = "file.out";
  private static final String INDEX_FILE_NAME = "file.out.index";

  public static final HttpResponseStatus TOO_MANY_REQ_STATUS =
      new HttpResponseStatus(429, "TOO MANY REQUESTS");
  // This should be kept in sync with Fetcher.FETCH_RETRY_DELAY_DEFAULT
  public static final long FETCH_RETRY_DELAY = 1000L;
  public static final String RETRY_AFTER_HEADER = "Retry-After";

  private int port;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final ChannelGroup allChannels =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private SSLFactory sslFactory;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected JobTokenSecretManager secretManager;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected Map<String, String> userRsrc;

  private DB stateDb = null;

  public static final String MAPREDUCE_SHUFFLE_SERVICEID =
      "mapreduce_shuffle";

  public static final String SHUFFLE_PORT_CONFIG_KEY = "mapreduce.shuffle.port";
  public static final int DEFAULT_SHUFFLE_PORT = 13562;

  public static final String SHUFFLE_LISTEN_QUEUE_SIZE =
      "mapreduce.shuffle.listen.queue.size";
  public static final int DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE = 128;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED =
      "mapreduce.shuffle.connection-keep-alive.enable";
  public static final boolean DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED = false;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT =
      "mapreduce.shuffle.connection-keep-alive.timeout";
  public static final int DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT = 5; //seconds

  public static final String SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      "mapreduce.shuffle.mapoutput-info.meta.cache.size";
  public static final int DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      1000;

  public static final String CONNECTION_CLOSE = "close";

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
      "mapreduce.shuffle.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  public static final String MAX_SHUFFLE_CONNECTIONS = "mapreduce.shuffle.max.connections";
  public static final int DEFAULT_MAX_SHUFFLE_CONNECTIONS = 0; // 0 implies no limit
  
  public static final String MAX_SHUFFLE_THREADS = "mapreduce.shuffle.max.threads";
  // 0 implies Netty default of 2 * number of available processors
  public static final int DEFAULT_MAX_SHUFFLE_THREADS = 0;
  
  public static final String SHUFFLE_BUFFER_SIZE = 
      "mapreduce.shuffle.transfer.buffer.size";
  public static final int DEFAULT_SHUFFLE_BUFFER_SIZE = 128 * 1024;
  
  public static final String  SHUFFLE_TRANSFERTO_ALLOWED = 
      "mapreduce.shuffle.transferTo.allowed";
  public static final boolean DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = true;
  public static final boolean WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = 
      false;
  static final String TIMEOUT_HANDLER = "timeout";

  /* the maximum number of files a single GET request can
   open simultaneously during shuffle
   */
  public static final String SHUFFLE_MAX_SESSION_OPEN_FILES =
      "mapreduce.shuffle.max.session-open-files";
  public static final int DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES = 3;


  /**
   * Shuffle服务指标收集器，统计Shuffle输出相关指标，同时作为Channel操作完成的监听器更新指标。
   */
  @Metrics(about="Shuffle output metrics", context="mapred")
  static class ShuffleMetrics implements ChannelFutureListener {
    @Metric("Shuffle output in bytes")
        MutableCounterLong shuffleOutputBytes;
    @Metric("# of failed shuffle outputs")
        MutableCounterInt shuffleOutputsFailed;
    @Metric("# of succeeeded shuffle outputs")
        MutableCounterInt shuffleOutputsOK;
    @Metric("# of current shuffle connections")
        MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final MetricsSystem ms;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  final ShuffleMetrics metrics;

  /**
   * 构造函数，使用指定的指标系统初始化ShuffleHandler。
   * @param ms 指标系统实例
   */
  ShuffleHandler(MetricsSystem ms) {
    super(MAPREDUCE_SHUFFLE_SERVICEID);
    this.ms = ms;
    metrics = ms.register(new ShuffleMetrics());
  }

  /**
   * 默认构造函数，使用默认指标系统初始化ShuffleHandler。
   */
  public ShuffleHandler() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * 将Shuffle服务端口序列化为ByteBuffer，供ApplicationMaster获取。
   * @param port Shuffle服务监听端口
   * @return 序列化后的端口字节缓冲区
   * @throws IOException 序列化失败时抛出
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer portDob = new DataOutputBuffer();
    portDob.writeInt(port);
    return ByteBuffer.wrap(portDob.getData(), 0, portDob.getLength());
  }

  /**
   * 反序列化Shuffle服务元数据，获取监听端口。
   * @param meta 序列化后的元数据缓冲区
   * @return Shuffle服务监听端口
   * @throws IOException 反序列化失败时抛出
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    int port = in.readInt();
    return port;
  }

  /**
   * 将作业Token序列化为ByteBuffer，作为服务数据传递给ShuffleHandler。
   * @param jobToken 作业认证Token
   * @return 序列化后的Token字节缓冲区
   * @throws IOException 序列化失败时抛出
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken)
      throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer jobTokenDob = new DataOutputBuffer();
    jobToken.write(jobTokenDob);
    return ByteBuffer.wrap(jobTokenDob.getData(), 0, jobTokenDob.getLength());
  }

  /**
   * 反序列化服务数据，获取作业认证Token。
   * @param secret 序列化后的Token字节缓冲区
   * @return 反序列化后的作业Token
   * @throws IOException 反序列化失败时抛出
   */
  public static Token<JobTokenIdentifier> deserializeServiceData(ByteBuffer secret)
      throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(secret);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.readFields(in);
    return jt;
  }

  @Override
  /**
   * 初始化应用程序，注册作业Shuffle信息和认证Token。
   * @param context 应用初始化上下文，包含用户、应用ID、应用数据
   */
  public void initializeApplication(ApplicationInitializationContext context) {

    String user = context.getUser();
    ApplicationId appId = context.getApplicationId();
    ByteBuffer secret = context.getApplicationDataForService();
    // TODO these bytes should be versioned
    try {
      Token<JobTokenIdentifier> jt = deserializeServiceData(secret);
       // TODO: Once SHuffle is out of NM, this can use MR APIs
      JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
      recordJobShuffleInfo(jobId, user, jt);
    } catch (IOException e) {
      LOG.error("Error during initApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  /**
   * 停止应用程序，清理作业Shuffle信息和认证Token。
   * @param context 应用终止上下文，包含应用ID
   */
  public void stopApplication(ApplicationTerminationContext context) {
    ApplicationId appId = context.getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    try {
      removeJobShuffleInfo(jobId);
    } catch (IOException e) {
      LOG.error("Error during stopApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  /**
   * 初始化Shuffle服务，创建Netty事件循环组。
   * @param conf 配置对象
   * @throws Exception 初始化失败时抛出
   */
  protected void serviceInit(Configuration conf) throws Exception {
    int maxShuffleThreads = conf.getInt(MAX_SHUFFLE_THREADS,
                                        DEFAULT_MAX_SHUFFLE_THREADS);
    // Since Netty 4.x, the value of 0 threads would default to:
    // io.netty.channel.MultithreadEventLoopGroup.DEFAULT_EVENT_LOOP_THREADS
    // by simply passing 0 value to NioEventLoopGroup constructor below.
    // However, this logic to determinte thread count
    // was in place so we can keep it for now.
    if (maxShuffleThreads == 0) {
      maxShuffleThreads = 2 * Runtime.getRuntime().availableProcessors();
    }

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Worker #%d")
        .build();
    
    bossGroup = new NioEventLoopGroup(1, bossFactory);
    workerGroup = new NioEventLoopGroup(maxShuffleThreads, workerFactory);
    super.serviceInit(new Configuration(conf));
  }

  /**
   * 创建Shuffle通道处理器上下文，初始化路径缓存等组件。
   * @return 初始化完成的处理器上下文
   */
  protected ShuffleChannelHandlerContext createHandlerContext() {
    Configuration conf = getConfig();