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

/**
 * @file MapOutputSpec.cc
 * MapReduce本地任务Map端输出配置规范实现，负责从配置中解析Map输出相关参数
 */

#include "lib/commons.h"
#include "lib/MapOutputSpec.h"
#include "NativeTask.h"

namespace NativeTask {

/**
 * @brief 从配置对象中解析Map输出规范，填充到spec对象中
 * @param config 配置对象指针，包含Map输出相关配置参数
 * @param spec 输出参数对象，解析后的配置将写入该对象
 */
void MapOutputSpec::getSpecFromConfig(Config * config, MapOutputSpec & spec) {
  if (NULL == config) {
    return;
  }
  // 默认使用CRC32校验和
  spec.checksumType = CHECKSUM_CRC32;
  // 获取排序算法类型配置
  string sortType = config->get(NATIVE_SORT_TYPE, "DUALPIVOTSORT");
  if (sortType == "DUALPIVOTSORT") {
    spec.sortAlgorithm = DUALPIVOTSORT;
  } else {
    spec.sortAlgorithm = CPPSORT;
  }
  // 判断是否开启Map输出压缩，获取压缩编解码器
  if (config->get(MAPRED_COMPRESS_MAP_OUTPUT, "false") == "true") {
    spec.codec = config->get(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC);
  } else {
    spec.codec = "";
  }
  // 判断是否需要排序，设置排序模式
  if (config->getBool(MAPRED_SORT_AVOID, false)) {
    spec.sortOrder = NOSORT;
  } else {
    spec.sortOrder = FULLORDER;
  }
  // 获取Map输出Key类型，先读Map输出专属配置，不存在则读全局输出配置
  const char * key_class = config->get(MAPRED_MAPOUTPUT_KEY_CLASS);
  if (NULL == key_class) {
    key_class = config->get(MAPRED_OUTPUT_KEY_CLASS);
  }
  if (NULL == key_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.key.class not set");
  }
  // 将Java类名转换为本地键值对类型标识
  spec.keyType = JavaClassToKeyValueType(key_class);
  // 获取Map输出Value类型，先读Map输出专属配置，不存在则读全局输出配置
  const char * value_class = config->get(MAPRED_MAPOUTPUT_VALUE_CLASS);
  if (NULL == value_class) {
    value_class = config->get(MAPRED_OUTPUT_VALUE_CLASS);
  }
  if (NULL == value_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.value.class not set");
  }
  // 将Java类名转换为本地键值对类型标识
  spec.valueType = JavaClassToKeyValueType(value_class);
}

} // namespace NativeTask