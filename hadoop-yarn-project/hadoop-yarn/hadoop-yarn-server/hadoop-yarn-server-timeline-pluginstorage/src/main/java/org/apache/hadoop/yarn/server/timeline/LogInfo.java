// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * 时间线服务日志文件信息抽象基类，用于封装日志文件基本属性，提供日志解析和存储的通用逻辑
 */
abstract class LogInfo {
  /** 实体文件名分隔符，用于匹配分组ID */
  public static final String ENTITY_FILE_NAME_DELIMITERS = "_.";

  public String getAttemptDirName() {
    return attemptDirName;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long newOffset) {
    this.offset = newOffset;
  }


  public long getLastProcessedTime() {
    return lastProcessedTime;
  }

  public void setLastProcessedTime(long lastProcessedTime) {
    this.lastProcessedTime = lastProcessedTime;
  }

  // 尝试目录名称
  private String attemptDirName;
  // 上次处理时间，初始值-1表示未处理
  private long lastProcessedTime = -1;
  // 日志文件名
  private String filename;
  // 日志所属用户
  private String user;
  // 当前已处理偏移量
  private long offset = 0;

  private static final Logger LOG = LoggerFactory.getLogger(LogInfo.class);

  /**
   * 构造日志信息对象
   * @param attemptDirName 尝试目录名称
   * @param file 日志文件名
   * @param owner 日志所属用户
   */
  public LogInfo(String attemptDirName, String file, String owner) {
    this.attemptDirName = attemptDirName;
    filename = file;
    user = owner;
  }

  /**
   * 获取日志文件完整路径
   * @param rootPath 应用根目录路径
   * @return 日志文件完整路径
   */
  public Path getPath(Path rootPath) {
    Path attemptPath = new Path(rootPath, attemptDirName);
    return new Path(attemptPath, filename);
  }

  public String getFilename() {
    return filename;
  }

  public boolean matchesGroupId(TimelineEntityGroupId groupId) {
    return matchesGroupId(groupId.toString());
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  boolean matchesGroupId(String groupId){
    // 检查分组ID是否为文件名的分隔片段（被分隔符分隔或位于文件名末尾）
    int pos = filename.indexOf(groupId);
    if (pos < 0) {
      return false;
    }
    return filename.length() == pos + groupId.length()
        || ENTITY_FILE_NAME_DELIMITERS.contains(String.valueOf(
        filename.charAt(pos + groupId.length())
    ));
  }

  /**
   * 解析日志文件并将数据存储到时间线数据管理器
   * @param tdm 时间线数据管理器
   * @param appDirPath 应用目录路径
   * @param appCompleted 应用是否已完成
   * @param jsonFactory JSON工厂
   * @param objMapper JSON对象映射器
   * @param fs HDFS文件系统
   * @return 解析出的实体/域数量
   * @throws IOException IO异常
   */
  public long parseForStore(TimelineDataManager tdm, Path appDirPath,
      boolean appCompleted, JsonFactory jsonFactory, ObjectMapper objMapper,
      FileSystem fs) throws IOException {
    LOG.debug("Parsing for log dir {} on attempt {}", appDirPath,
        attemptDirName);
    // 获取日志文件路径
    Path logPath = getPath(appDirPath);
    // 获取文件状态
    FileStatus status = fs.getFileStatus(logPath);
    // 解析计数
    long numParsed = 0;
    if (status != null) {
      // 获取当前文件修改时间
      long curModificationTime = status.getModificationTime();
      // 仅在文件修改后才重新解析
      if (curModificationTime > getLastProcessedTime()) {
        long startTime = Time.monotonicNow();
        try {
          LOG.info("Parsing {} at offset {}", logPath, offset);
          // 执行实际解析
          long count =
              parsePath(tdm, logPath, appCompleted, jsonFactory, objMapper, fs);
          // 更新上次处理时间
          setLastProcessedTime(curModificationTime);
          LOG.info("Parsed {} entities from {} in {} msec", count, logPath,
              Time.monotonicNow() - startTime);
          numParsed += count;
        } catch (RuntimeException e) {
          // 处理解析异常，判断文件损坏情况
          if (e.getCause() instanceof JsonParseException
              && (status.getLen() > 0 || offset > 0)) {
            // 文件非空或者已读取过，判定为损坏跳过
            LOG.info("Log {} appears to be corrupted. Skip. ", logPath);
          } else {
            // 其他错误打印错误日志
            LOG.error("Failed to parse " + logPath + " from offset " + offset,
                e);
          }
        }
      } else {
        // 文件无修改跳过解析
        LOG.info("Skip Parsing {} as there is no change", logPath);
      }
    } else {
      // 文件不存在警告跳过
      LOG.warn("{} no longer exists. Skip for scanning. ", logPath);
    }
    return numParsed;
  }

  /**
   * 打开并解析指定日志路径
   * @param tdm 时间线数据管理器
   * @param logPath 日志文件路径
   * @param appCompleted 应用是否已完成
   * @param jsonFactory JSON工厂
   * @param objMapper JSON对象映射器
   * @param fs HDFS文件系统
   * @return 解析出的实体/域数量
   * @throws IOException IO异常
   */
  private long parsePath(TimelineDataManager tdm, Path logPath,
      boolean appCompleted, JsonFactory jsonFactory, ObjectMapper objMapper,
      FileSystem fs) throws IOException {
    // 创建对应用户的UGI
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(user);
    // 打开文件输入流
    FSDataInputStream in = fs.open(logPath);
    JsonParser parser = null;
    try {
      // 跳转到已处理偏移量
      in.seek(offset);
      try {
        // 创建JSON解析器，不自动关闭输入流
        parser = jsonFactory.createParser((InputStream)in);
        parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
      } catch (IOException e) {
        // 应用未完成时，不完整文件解析错误按EOF处理，返回已解析数量
        if (appCompleted) {
          throw e;
        } else {
          LOG.debug("Exception in parse path: {}", e.getMessage());
          return 0;
        }
      }

      // 调用抽象方法执行实际解析
      return doParse(tdm, parser, objMapper, ugi, appCompleted);
    } finally {
      // 清理资源
      IOUtils.closeStream(parser);
      IOUtils.closeStream(in);
    }
  }

  /**
   * 抽象解析方法，由子类实现具体解析逻辑
   * @param tdm 时间线数据管理器
   * @param parser JSON解析器
   * @param objMapper JSON对象映射器
   * @param ugi 用户信息
   * @param appCompleted 应用是否已完成
   * @return 解析出的实体/域数量
   * @throws IOException IO异常
   */
  protected abstract long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException;
}

/**
 * 实体日志信息类，实现实体日志的解析和存储逻辑
 */
class EntityLogInfo extends LogInfo {
  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);

  /**
   * 构造实体日志信息对象
   * @param attemptId 尝试ID
   * @param file 日志文件名
   * @param owner 日志所属用户
   */
  public EntityLogInfo(String attemptId,
      String file, String owner) {
    super(attemptId, file, owner);
  }

  @Override
  protected long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException {
    long count = 0;
    TimelineEntities entities = new TimelineEntities();
    ArrayList<TimelineEntity> entityList = new ArrayList<TimelineEntity>(1);
    boolean postError = false;
    try {
      // 获取实体迭代器
      MappingIterator<TimelineEntity> iter = objMapper.readValues(parser,
          TimelineEntity.class);
      long curPos;
      // 逐实体解析
      while (iter.hasNext()) {
        TimelineEntity entity = iter.next();
        String etype = entity.getEntityType();
        String eid = entity.getEntityId();
        LOG.debug("Read entity {} of {}", eid, etype);
        ++count;
        // 获取当前解析位置
        curPos = ((FSDataInputStream) parser.getInputSource()).getPos();
        LOG.debug("Parser now at offset {}", curPos);

        try {
          LOG.debug("Adding {}({}) to store", eid, etype);
          // 将实体存入时间线存储
          entityList.add(entity);
          entities.setEntities(entityList);
          TimelinePutResponse response = tdm.postEntities(entities, ugi);
          // 打印存储错误日志
          for (TimelinePutResponse.TimelinePutError e
              : response.getErrors()) {
            LOG.warn("Error putting entity: {} ({}): {}",
                e.getEntityId(), e.getEntityType(), e.getErrorCode());
          }
          // 更新偏移量
          setOffset(curPos);
          entityList.clear();
        } catch (YarnException e) {
          postError = true;
          throw new IOException("Error posting entities", e);
        } catch (IOException e) {
          postError = true;
          throw new IOException("Error posting entities", e);
        }
      }
    } catch (IOException e) {
      // 应用未完成时，不完整文件解析错误按EOF处理，不抛出异常
      if (appCompleted || postError) {
        throw e;
      }
    } catch (RuntimeException e) {
      // 仅在应用完成或非JSON解析异常时抛出
      if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
        throw e;
      }
    }
    return count;
  }
}

/**
 * 域日志信息类，实现域日志的解析和存储逻辑
 */
class DomainLogInfo extends LogInfo {
  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);

  /**
   * 构造域日志信息对象
   * @param attemptDirName 尝试目录名称
   * @param file 日志文件名
   * @param owner 日志所属用户
   */
  public DomainLogInfo(String attemptDirName, String file,
      String owner) {
    super(attemptDirName, file, owner);
  }

  @Override
  protected long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException {
    long count = 0;
    long curPos;
    boolean putError = false;
    try {
      // 获取域迭代器
      MappingIterator<TimelineDomain> iter = objMapper.readValues(parser,
          TimelineDomain.class);

      // 逐域解析
      while (iter.hasNext()) {
        TimelineDomain domain = iter.next();
        // 设置域所有者为当前用户
        domain.setOwner(ugi.getShortUserName());
        LOG.trace("Read domain {}", domain.getId());
        ++count;
        // 获取当前解析位置
        curPos = ((FSDataInputStream) parser.getInputSource()).getPos();
        LOG.debug("Parser now at offset {}", curPos);

        try {
          // 将域存入时间线存储
          tdm.putDomain(domain, ugi);
          // 更新偏移量
          setOffset(curPos);
        } catch (YarnException e) {
          putError = true;
          throw new IOException("Error posting domain", e);
        } catch (IOException e) {
          putError = true;
          throw new IOException("Error posting domain", e);
        }
      }
    } catch (IOException e) {
      // 应用未完成时，不完整文件解析错误按EOF处理，不抛出异常
      if (appCompleted || putError) {
        throw e;
      }
    } catch (RuntimeException e) {
      // 仅在应用完成或非JSON解析异常时抛出
      if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
        throw e;
      }
    }
    return count;
  }
}