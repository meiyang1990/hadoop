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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HDFS FSImage离线查看工具的FSImage加载器接口，负责从FSImage输入流中解析结构，并调用访问者处理各节点。
 * 
 * 设计思路：不同FSImage布局版本可对应不同实现，当版本变更较大时新增实现，保证每个加载器的处理效率，避免兼容逻辑过多导致性能下降。
 * 核心职责：将FSImage二进制文件解析后，通过访问者模式把结构交给外部处理器处理。
 */
interface ImageLoader {

  /**
   * 从输入流加载FSImage，遍历文件结构并调用访问者处理各个节点。
   * @param in 指向FSImage文件的输入流
   * @param v 用于处理FSImage结构的访问者
   * @param enumerateBlocks 是否需要遍历访问每个文件的块信息
   * @throws IOException 读取文件或访问过程中发生IO异常
   */
  public void loadImage(DataInputStream in, ImageVisitor v,
      boolean enumerateBlocks) throws IOException;

  /**
   * 判断当前加载器是否支持处理指定版本的FSImage。
   * @param version FSImage布局版本号
   * @return 当前加载器能否处理该版本，返回true表示支持
   */
  public boolean canLoadVersion(int version);

  /**
   * ImageLoader工厂类，根据FSImage版本号获取对应的加载器实例。
   * 由于Java接口不支持静态方法，因此将工厂实现为接口内部的静态类。
   */
  @InterfaceAudience.Private
  public class LoaderFactory {

    /**
     * 根据FSImage布局版本号，获取对应支持该版本的加载器实例。
     * @param version 需要处理的FSImage布局版本号
     * @return 支持该版本的ImageLoader，找不到对应加载器则返回null
     */
    static public ImageLoader getLoader(int version) {
      // 所有已实现的加载器列表，新增版本加载器可直接在此添加
      ImageLoader[] loaders = { new ImageLoaderCurrent() };

      for (ImageLoader l : loaders) {
        if (l.canLoadVersion(version))
          return l;
      }

      return null;
    }
  }
}