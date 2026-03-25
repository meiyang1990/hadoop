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
 * @file Constants.h
 * @brief MapReduce本地任务模块公共常量定义头文件
 *
 * 定义了本地任务处理过程中键值对序列化相关的固定长度常量，
 * 供本机MapReduce任务的IO读写模块使用
 */
#ifndef CONSTANTS_H_
#define CONSTANTS_H_

// 分区编号长度字段占用字节大小
const uint32_t SIZE_OF_PARTITION_LENGTH = sizeof(uint32_t);
// 键长度字段占用字节大小
const uint32_t SIZE_OF_KEY_LENGTH = sizeof(uint32_t);
// 值长度字段占用字节大小
const uint32_t SIZE_OF_VALUE_LENGTH = sizeof(uint32_t);
// 键值对总长度字段（键长度+值长度）占用字节大小
const uint32_t SIZE_OF_KV_LENGTH = SIZE_OF_KEY_LENGTH + SIZE_OF_VALUE_LENGTH;

#endif //CONSTANTS_H_