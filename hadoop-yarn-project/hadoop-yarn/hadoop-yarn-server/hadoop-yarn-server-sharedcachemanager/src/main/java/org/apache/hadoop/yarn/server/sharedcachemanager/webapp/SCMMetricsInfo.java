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

package org.apache.hadoop.yarn.server.sharedcachemanager.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;

// 这个文件已经全部加上中文注释
// 用于汇总共享缓存管理器指标以供WebUI显示的JavaBean类
/**
 * 共享缓存管理器(SCM)指标信息封装类，用于聚合各类指标供Web UI展示
 * 
 * 聚合了清理器、客户端、上传器三类核心指标，通过JAXB支持XML序列化返回给Web前端
 */
@XmlRootElement(name = "SCMMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
@Private
@Unstable
public class SCMMetricsInfo {
  // 累计删除文件数
  protected long totalDeletedFiles;
  // 累计处理文件数
  protected long totalProcessedFiles;
  // 缓存命中次数
  protected long cacheHits;
  // 缓存未命中次数
  protected long cacheMisses;
  // 缓存释放次数
  protected long cacheReleases;
  // 接受的上传数
  protected long acceptedUploads;
  // 拒绝的上传数
  protected long rejectedUploads;

  public SCMMetricsInfo() {
  }
  
  /**
   * 从各组件指标构造聚合后的SCM指标信息
   * @param cleanerMetrics 清理器指标实例
   * @param clientSCMMetrics 客户端指标实例
   * @param scmUploaderMetrics 上传器指标实例
   */
  public SCMMetricsInfo(CleanerMetrics cleanerMetrics,
      ClientSCMMetrics clientSCMMetrics,
      SharedCacheUploaderMetrics scmUploaderMetrics) {
    totalDeletedFiles = cleanerMetrics.getTotalDeletedFiles();
    totalProcessedFiles = cleanerMetrics.getTotalProcessedFiles();
    cacheHits = clientSCMMetrics.getCacheHits();
    cacheMisses = clientSCMMetrics.getCacheMisses();
    cacheReleases = clientSCMMetrics.getCacheReleases();
    acceptedUploads = scmUploaderMetrics.getAcceptedUploads();
    rejectedUploads = scmUploaderMetrics.getRejectUploads();
  }

  public long getTotalDeletedFiles() { return totalDeletedFiles; }
  public long getTotalProcessedFiles() { return totalProcessedFiles; }
  public long getCacheHits() { return cacheHits; }
  public long getCacheMisses() { return cacheMisses; }
  public long getCacheReleases() { return cacheReleases; }
  public long getAcceptedUploads() { return acceptedUploads; }
  public long getRejectUploads() { return rejectedUploads; }
}