// 这个文件已经全部加上中文注释
/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Map;

/**
 * OCI镜像清单规范的Java实现类，用于解析和存储runc容器镜像的清单信息。
 */
@InterfaceStability.Unstable
public class ImageManifest {
  // 清单schema版本号
  final private int schemaVersion;
  // 清单媒体类型
  final private String mediaType;
  // 镜像配置Blob对象
  final private Blob config;
  // 镜像分层Blob列表
  final private ArrayList<Blob> layers;
  // 镜像注解信息
  final private Map<String, String> annotations;

  /**
   * 默认构造函数，使用空参数初始化。
   */
  public ImageManifest() {
    this(0, null, null, null, null);
  }

  /**
   * 全参数构造函数，初始化镜像清单所有字段。
   * @param schemaVersion 清单schema版本号
   * @param mediaType 清单媒体类型
   * @param config 镜像配置Blob
   * @param layers 镜像分层Blob列表
   * @param annotations 镜像注解信息
   */
  public ImageManifest(int schemaVersion, String mediaType, Blob config,
      ArrayList<Blob> layers, Map<String, String> annotations) {
    this.schemaVersion = schemaVersion;
    this.mediaType = mediaType;
    this.config = config;
    this.layers = layers;
    this.annotations = annotations;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public String getMediaType() {
    return mediaType;
  }

  public Blob getConfig() {
    return config;
  }

  public ArrayList<Blob> getLayers() {
    return layers;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("schemaVersion: " + schemaVersion + "\n");
    sb.append("mediaType: " + mediaType + "\n");
    sb.append(config.toString());
    for(Blob b : layers) {
      sb.append(b.toString());
    }
    return sb.toString();
  }

  /**
   * OCI镜像Blob的Java实现类，代表镜像中的数据块（配置或分层）。
   */
  @InterfaceStability.Unstable
  public static class Blob {
    // Blob媒体类型
    final private String mediaType;
    // Blob内容摘要
    final private String digest;
    // Blob大小（字节）
    final private long size;
    // Blob下载地址列表
    final private ArrayList<String> urls;
    // Blob注解信息
    final private Map<String, String> annotations;

    /**
     * 默认构造函数，使用空参数初始化。
     */
    public Blob() {
      this(null, null, 0, null, null);
    }

    /**
     * 全参数构造函数，初始化Blob所有字段。
     * @param mediaType Blob媒体类型
     * @param digest Blob内容摘要
     * @param size Blob大小（字节）
     * @param urls Blob下载地址列表
     * @param annotations Blob注解信息
     */
    public Blob(String mediaType, String digest, long size,
        ArrayList<String> urls, Map<String, String> annotations) {
      this.mediaType = mediaType;
      this.digest = digest;
      this.size = size;
      this.urls = urls;
      this.annotations = annotations;
    }

    public String getMediaType() {
      return mediaType;
    }

    public String getDigest() {
      return digest;
    }

    public long getSize() {
      return size;
    }

    public ArrayList<String> getUrls() {
      return urls;
    }

    public Map<String, String> getAnnotations() {
      return annotations;
    }

    @Override
    public String toString() {
      return "mediaType: " + mediaType + "\n" + "size: " + size + "\n"
          + "digest: " + digest + "\n";
    }
  }
}