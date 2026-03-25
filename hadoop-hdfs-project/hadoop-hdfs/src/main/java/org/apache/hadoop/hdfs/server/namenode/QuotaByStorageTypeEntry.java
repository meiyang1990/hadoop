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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.StringUtils;

/**
 * 按存储介质类型划分的配额条目，保存HDFS目录对应某种存储类型的配额信息
 * 用于HDFS目录级按存储介质类型配额管理，记录指定存储类型的配额值
 */
public class QuotaByStorageTypeEntry {
   private StorageType type;
   private long quota;

   /**
    * 获取该条目对应的存储类型
    * @return 存储类型枚举
    */
   public StorageType getStorageType() {
     return type;
   }

   /**
    * 获取该存储类型设置的配额值
    * @return 配额数值
    */
   public long getQuota() {
     return quota;
   }

   @Override
   public boolean equals(Object o){
     if (o == null) {
       return false;
     }
     if (getClass() != o.getClass()) {
       return false;
     }
     QuotaByStorageTypeEntry other = (QuotaByStorageTypeEntry)o;
     return Objects.equal(type, other.type) && Objects.equal(quota, other.quota);
   }

   @Override
   public int hashCode() {
     return Objects.hashCode(type, quota);
   }

   @Override
   public String toString() {
     StringBuilder sb = new StringBuilder();
     assert (type != null);
    sb.append(StringUtils.toLowerCase(type.toString()))
        .append(':')
        .append(quota);
     return sb.toString();
   }

   /**
    * QuotaByStorageTypeEntry的Builder构造器，用于构建配额条目对象
    */
   public static class Builder {
     private StorageType type;
     private long quota;

     /**
      * 设置配额对应的存储类型
      * @param type 存储类型枚举
      * @return 当前Builder实例
      */
     public Builder setStorageType(StorageType type) {
       this.type = type;
       return this;
     }

     /**
      * 设置该存储类型的配额值
      * @param quota 配额数值
      * @return 当前Builder实例
      */
     public Builder setQuota(long quota) {
       this.quota = quota;
       return this;
     }

     /**
      * 构建QuotaByStorageTypeEntry实例
      * @return 构建完成的配额条目对象
      */
     public QuotaByStorageTypeEntry build() {
       return new QuotaByStorageTypeEntry(type, quota);
     }
   }

   /**
    * 私有构造函数，通过Builder创建配额条目
    * @param type 存储类型
    * @param quota 配额值
    */
   private QuotaByStorageTypeEntry(StorageType type, long quota) {
     this.type = type;
     this.quota = quota;
   }
 }