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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CryptoUtils;

/**
 * MapReduce溢写文件追踪类，负责记录、校验溢写文件路径与偏移量，用于安全校验和问题排查。
 * 继承自SpillCallBackInjector，实现各个溢写操作的回调收集逻辑。
 */
public class SpillCallBackPathsFinder extends SpillCallBackInjector {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpillCallBackPathsFinder.class);
  /**
   * 加密溢写文件集合，key为溢写文件路径，value为文件中记录的起始偏移量集合
   */
  private final Map<Path, Set<Long>> encryptedSpillFiles =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * 非加密溢写文件集合，key为溢写文件路径，value为文件中记录的起始偏移量集合
   */
  private final Map<Path, Set<Long>> spillFiles =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * 非法位置访问记录，记录不符合预期的读取位置，用于异常检测
   */
  private final Map<Path, Set<Long>> invalidAccessMap =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * 溢写索引文件路径集合
   */
  private final Set<Path> indexSpillFiles = ConcurrentHashMap.newKeySet();
  /**
   * 未找到的溢写文件缓存，记录查询不到的路径避免重复日志
   */
  private final Set<Path> negativeCache = ConcurrentHashMap.newKeySet();

  /**
   * 根据配置是否开启加密溢写，返回对应的文件存储Map
   * @param config Hadoop配置对象
   * @return 加密或非加密溢写文件存储Map
   */
  protected Map<Path, Set<Long>> getFilesMap(Configuration config) {
    if (CryptoUtils.isEncryptedSpillEnabled(config)) {
      return encryptedSpillFiles;
    }
    return spillFiles;
  }

  @Override
  /**
   * 写入溢写文件回调，记录溢写文件路径与输出偏移量
   * @param path 溢写文件路径
   * @param out 输出流对象
   * @param conf Hadoop配置对象
   */
  public void writeSpillFileCB(Path path, FSDataOutputStream out,
      Configuration conf) {
    long outPos = out.getPos();
    getFilesMap(conf)
        .computeIfAbsent(path, p -> ConcurrentHashMap.newKeySet())
        .add(outPos);
    LOG.debug("writeSpillFileCB.. path:{}; pos:{}", path, outPos);
  }

  @Override
  /**
   * 读取溢写文件回调，校验读取位置是否合法，记录非法访问
   * @param path 溢写文件路径
   * @param is 输入流对象
   * @param conf Hadoop配置对象
   */
  public void getSpillFileCB(Path path, InputStream is, Configuration conf) {
    if (path == null) {
      return;
    }
    Set<Long> pathEntries = getFilesMap(conf).get(path);
    if (pathEntries != null) {
      try {
        long isPos = CryptoStreamUtils.getInputStreamOffset(is);
        if (pathEntries.contains(isPos)) {
          LOG.debug("getSpillFileCB... Path {}; Pos: {}", path, isPos);
          return;
        }
        // 记录不匹配的读取位置为非法访问
        invalidAccessMap
            .computeIfAbsent(path, p -> ConcurrentHashMap.newKeySet())
            .add(isPos);
        LOG.debug("getSpillFileCB... access incorrect position.. "
            + "Path {}; Pos: {}", path, isPos);
      } catch (IOException e) {
        LOG.error("Could not get inputStream position.. Path {}", path, e);
        // 获取偏移量失败不阻断流程
      }
      return;
    }
    // 路径不存在加入负缓存，记录警告日志
    negativeCache.add(path);
    LOG.warn("getSpillFileCB.. Could not find spilled file .. Path: {}", path);
  }

  @Override
  /**
   * 生成所有溢写相关信息的诊断报告，用于问题排查
   * @return 格式化后的溢写诊断报告字符串
   */
  public String getSpilledFileReport() {
    StringBuilder strBuilder =
        new StringBuilder("\n++++++++ Spill Report ++++++++")
            .append(dumpMapEntries("Encrypted Spilled Files",
                encryptedSpillFiles))
            .append(dumpMapEntries("Non-Encrypted Spilled Files",
                spillFiles))
            .append(dumpMapEntries("Invalid Spill Access",
                invalidAccessMap))
            .append("\n ----- Spilled Index Files ----- ")
            .append(indexSpillFiles.size());
    // 遍历所有索引文件路径添加到报告
    for (Path p : indexSpillFiles) {
      strBuilder.append("\n\t index-path: ").append(p.toString());
    }
    // 添加负缓存中未找到的路径信息
    strBuilder.append("\n ----- Negative Cache files ----- ")
        .append(negativeCache.size());
    for (Path p : negativeCache) {
      strBuilder.append("\n\t path: ").append(p.toString());
    }
    return strBuilder.toString();
  }

  @Override
  /**
   * 添加溢写索引文件回调，记录索引文件路径
   * @param path 索引文件路径
   * @param conf Hadoop配置对象
   */
  public void addSpillIndexFileCB(Path path, Configuration conf) {
    if (path == null) {
      return;
    }
    indexSpillFiles.add(path);
    LOG.debug("addSpillIndexFileCB... Path: {}", path);
  }

  @Override
  /**
   * 校验溢写索引文件是否存在回调，记录不存在的索引文件
   * @param path 待校验索引文件路径
   * @param conf Hadoop配置对象
   */
  public void validateSpillIndexFileCB(Path path, Configuration conf) {
    if (path == null) {
      return;
    }
    if (indexSpillFiles.contains(path)) {
      LOG.debug("validateSpillIndexFileCB.. Path: {}", path);
      return;
    }
    LOG.warn("validateSpillIndexFileCB.. could not retrieve indexFile.. "
        + "Path: {}", path);
    negativeCache.add(path);
  }

  /**
   * 获取所有加密溢写文件的不可修改路径集合
   * @return 加密溢写文件路径集合
   */
  public Set<Path> getEncryptedSpilledFiles() {
    return Collections.unmodifiableSet(encryptedSpillFiles.keySet());
  }

  /**
   * 获取所有非法溢写访问条目集合，每个条目格式为"路径[偏移量]"
   * @return 非法访问条目字符串集合
   */
  public Set<String> getInvalidSpillEntries() {
    Set<String> result = new LinkedHashSet<>();
    for (Entry<Path, Set<Long>> spillMapEntry: invalidAccessMap.entrySet()) {
      for (Long singleEntry : spillMapEntry.getValue()) {
        result.add(String.format("%s[%d]",
            spillMapEntry.getKey(), singleEntry));
      }
    }
    return result;
  }

  /**
   * 将Map中存储的溢写信息格式化为字符串，用于生成诊断报告
   * @param label 分类标签
   * @param entriesMap 待格式化的溢写信息Map
   * @return 格式化后的字符串
   */
  private String dumpMapEntries(String label,
      Map<Path, Set<Long>> entriesMap) {
    StringBuilder strBuilder =
        new StringBuilder(String.format("%n ----- %s ----- %d", label,
            entriesMap.size()));
    for (Entry<Path, Set<Long>> encryptedSpillEntry
        : entriesMap.entrySet()) {
      strBuilder.append(String.format("%n\t\tpath: %s",
          encryptedSpillEntry.getKey()));
      for (Long singlePos : encryptedSpillEntry.getValue()) {
        strBuilder.append(String.format("%n\t\t\tentry: %d", singlePos));
      }
    }
    return strBuilder.toString();
  }
}