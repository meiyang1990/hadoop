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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce Shuffle阶段的Map输出索引缓存，用于缓存Map任务输出的分段索引信息，
 * 避免重复读取索引文件，提升Shuffle阶段的性能。通过LRU策略控制内存使用。
 */
class IndexCache {

  private final JobConf conf;
  private final int totalMemoryAllowed;
  private AtomicInteger totalMemoryUsed = new AtomicInteger();
  private static final Logger LOG = LoggerFactory.getLogger(IndexCache.class);

  /** 索引信息缓存，key为Map任务ID，value为对应的索引信息 */
  private final ConcurrentHashMap<String,IndexInformation> cache =
    new ConcurrentHashMap<String,IndexInformation>();
  
  /** 索引访问队列，按访问顺序存储Map任务ID，用于LRU缓存淘汰 */
  private final LinkedBlockingQueue<String> queue = 
    new LinkedBlockingQueue<String>();

  /**
   * 构造索引缓存实例，根据配置初始化最大内存限制
   * @param conf 作业配置对象
   */
  public IndexCache(JobConf conf) {
    this.conf = conf;
    totalMemoryAllowed =
      conf.getInt(MRJobConfig.SHUFFLE_INDEX_CACHE, 10) * 1024 * 1024;
    LOG.info("IndexCache created with max memory = " + totalMemoryAllowed);
  }

  /**
   * 获取指定Map任务对应Reduce分区的索引信息，如果缓存未命中则读取索引文件到缓存
   * @param mapId Map任务ID
   * @param reduce Reduce分区编号
   * @param fileName 索引文件路径（缓存未命中时读取）
   * @param expectedIndexOwner 索引文件的预期所有者
   * @return 指定分区的索引记录，包含数据偏移量、压缩长度等信息
   * @throws IOException 读取索引文件或等待缓存构建失败时抛出
   */
  public IndexRecord getIndexInformation(String mapId, int reduce,
                                         Path fileName, String expectedIndexOwner)
    throws IOException {

    IndexInformation info = cache.get(mapId);

    if (info == null) {
      // 缓存未命中，读取索引文件构建缓存
      info = readIndexFileToCache(fileName, mapId, expectedIndexOwner);
    } else {
      // 缓存命中，等待索引构建完成（若正在构建）
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId " + mapId + " found");
    }

    // 校验索引请求合法性
    if (info.mapSpillRecord.size() == 0 ||
        info.mapSpillRecord.size() <= reduce) {
      throw new IOException("Invalid request " +
        " Map Id = " + mapId + " Reducer = " + reduce +
        " Index Info Length = " + info.mapSpillRecord.size());
    }
    return info.mapSpillRecord.getIndex(reduce);
  }

  /**
   * 检查索引信息是否正在构建中
   * @param info 索引信息对象
   * @return true表示索引尚未构建完成，false表示已构建完成
   */
  private boolean isUnderConstruction(IndexInformation info) {
    synchronized(info) {
      return (null == info.mapSpillRecord);
    }
  }

  /**
   * 从磁盘读取索引文件并将其添加到缓存中，处理并发缓存构建场景
   * @param indexFileName 索引文件路径
   * @param mapId Map任务ID
   * @param expectedIndexOwner 索引文件的预期所有者
   * @return 构建完成的索引信息对象
   * @throws IOException 读取索引文件失败或等待时被中断抛出
   */
  private IndexInformation readIndexFileToCache(Path indexFileName,
                                                String mapId,
                                                String expectedIndexOwner)
    throws IOException {
    IndexInformation info;
    IndexInformation newInd = new IndexInformation();
    // CAS原子放置新索引对象，避免并发重复构建
    if ((info = cache.putIfAbsent(mapId, newInd)) != null) {
      // 其他线程已经开始构建，等待完成
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId " + mapId + " found");
      return info;
    }
    LOG.debug("IndexCache MISS: MapId " + mapId + " not found") ;
    SpillRecord tmp = null;
    try { 
      // 读取索引文件内容
      tmp = new SpillRecord(indexFileName, conf, expectedIndexOwner);
    } catch (Throwable e) { 
      // 读取失败，初始化空索引并移除缓存条目
      tmp = new SpillRecord(0);
      cache.remove(mapId);
      throw new IOException("Error Reading IndexFile", e);
    } finally { 
      // 索引构建完成，通知所有等待线程
      synchronized (newInd) { 
        newInd.mapSpillRecord = tmp;
        newInd.notifyAll();
      } 
    } 
    // 将索引添加到访问队列
    queue.add(mapId);
    
    // 超过内存限制，触发LRU缓存淘汰
    if (totalMemoryUsed.addAndGet(newInd.getSize()) > totalMemoryAllowed) {
      freeIndexInformation();
    }
    return newInd;
  }

  /**
   * 从缓存中移除指定Map任务的索引信息，当该Map任务输出被丢弃时调用
   * @param mapId 要移除的Map任务ID
   */
  public void removeMap(String mapId) {
    IndexInformation info = cache.get(mapId);
    if (info == null || isUnderConstruction(info)) {
      return;
    }
    info = cache.remove(mapId);
    if (info != null) {
      // 减去对应内存占用
      totalMemoryUsed.addAndGet(-info.getSize());
      if (!queue.remove(mapId)) {
        LOG.warn("Map ID" + mapId + " not found in queue!!");
      }
    } else {
      LOG.info("Map ID " + mapId + " not found in cache");
    }
  }

  /**
   * 校验缓存内存使用统计是否一致，仅用于单元测试
   * @return 统计一致返回true，否则返回false
   */
  boolean checkTotalMemoryUsed() {
    int totalSize = 0;
    for (IndexInformation info : cache.values()) {
      totalSize += info.getSize();
    }
    return totalSize == totalMemoryUsed.get();
  }

  /**
   * 通过LRU策略淘汰最早访问的索引，将内存使用降低到限制以下
   */
  private synchronized void freeIndexInformation() {
    while (totalMemoryUsed.get() > totalMemoryAllowed) {
      // 移除队列头部最早访问的索引
      String s = queue.remove();
      IndexInformation info = cache.remove(s);
      if (info != null) {
        // 减去对应内存占用
        totalMemoryUsed.addAndGet(-info.getSize());
      }
    }
  }

  /**
   * 存储单个Map任务的所有输出分区索引信息，以及计算索引占用内存大小
   */
  private static class IndexInformation {
    SpillRecord mapSpillRecord;

    /**
     * 计算当前索引信息占用的内存大小（字节）
     * @return 索引占用内存字节数
     */
    int getSize() {
      return mapSpillRecord == null
        ? 0
        : mapSpillRecord.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    }
  }
}