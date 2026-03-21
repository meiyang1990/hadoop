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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.util.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * YARN RM Web DAO类，用于封装节点分配标签信息，供Web UI展示使用
 */
@XmlRootElement(name = "allocationTagsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class AllocationTagsInfo {

  // 存储单个分配标签信息的列表
  private ArrayList<AllocationTagInfo> allocationTagInfo;

  /**
   * 默认构造方法，初始化分配标签列表
   */
  public AllocationTagsInfo() {
    allocationTagInfo = new ArrayList<>();
  }

  /**
   * 添加单个分配标签信息到列表中
   * @param info 单个分配标签信息对象
   */
  public void addAllocationTag(AllocationTagInfo info) {
    allocationTagInfo.add(info);
  }

  /**
   * 将所有分配标签拼接为逗号分隔的字符串返回
   * @return 逗号分隔的分配标签字符串
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<AllocationTagInfo> it = allocationTagInfo.iterator();
    // 遍历所有分配标签，逐个拼接
    while (it.hasNext()) {
      AllocationTagInfo current = it.next();
      sb.append(current.toString());
      // 非最后一个元素后添加逗号分隔符
      if (it.hasNext()) {
        sb.append(StringUtils.COMMA);
      }
    }
    return sb.toString();
  }
}