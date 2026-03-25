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
 * @file SpillInfo.cc
 * @brief MapReduce本地任务溢写信息管理实现，管理map任务溢写到磁盘的数据元信息
 *
 * 本文件属于Hadoop MapReduce本地任务模块，负责处理Map阶段溢出数据的元信息持久化
 * 与清理，提供溢写段信息的序列化写入功能，为后续合并阶段提供索引支持
 */

#include "lib/commons.h"
#include "lib/Streams.h"
#include "lib/FileSystem.h"
#include "lib/Buffers.h"
#include "lib/SpillInfo.h"

namespace NativeTask {

/**
 * 删除当前溢写文件
 * 溢写任务完成后清理临时溢写文件，若文件路径非空且文件存在则删除
 */
void SingleSpillInfo::deleteSpillFile() {
  if (path.length() > 0) {
    struct stat st;
    if (0 == stat(path.c_str(), &st)) {
      remove(path.c_str());
    }
  }
}

/**
 * 将当前溢写信息序列化写入本地文件
 * @param filepath 溢写信息文件存储路径
 * 
 * 该方法将所有溢写数据段的偏移信息序列化到磁盘，添加CRC32校验保证数据完整性
 * 为后续合并阶段提供准确的溢写数据位置索引
 */
void SingleSpillInfo::writeSpillInfo(const std::string & filepath) {
  // 创建本地输出流，覆盖已有文件
  OutputStream * fout = FileSystem::getLocal().create(filepath, true);
  {
    // 包装带CRC32校验的输出流
    ChecksumOutputStream dest = ChecksumOutputStream(fout, CHECKSUM_CRC32);
    AppendBuffer appendBuffer;
    // 初始化32KB大小的写缓冲区
    appendBuffer.init(32 * 1024, &dest, "");
    uint64_t base = 0;

    // 遍历所有数据段，写入段偏移信息
    for (size_t j = 0; j < this->length; j++) {
      IFileSegment * segment = &(this->segments[j]);
      const bool firstSegment = (j == 0);
      if (firstSegment) {
        // 第一个段直接写入基准偏移、未压缩结束偏移、实际压缩结束偏移
        appendBuffer.write_uint64_be(base);
        appendBuffer.write_uint64_be(segment->uncompressedEndOffset);
        appendBuffer.write_uint64_be(segment->realEndOffset);
      } else {
        // 非第一个段，相对前一个段写入偏移增量，节省空间
        appendBuffer.write_uint64_be(base + this->segments[j - 1].realEndOffset);
        appendBuffer.write_uint64_be(
            segment->uncompressedEndOffset - this->segments[j - 1].uncompressedEndOffset);
        appendBuffer.write_uint64_be(segment->realEndOffset - this->segments[j - 1].realEndOffset);
      }
    }
    // 刷新缓冲区，将所有数据写入输出流
    appendBuffer.flush();
    // 获取整个溢写信息的CRC32校验值
    uint32_t chsum = dest.getChecksum();
#ifdef SPILLRECORD_CHECKSUM_UINT
    // 字节序转换后写入32位校验值
    chsum = bswap(chsum);
    fout->write(&chsum, sizeof(uint32_t));
#else
    // 字节序转换后写入64位校验值
    uint64_t wtchsum = bswap64((uint64_t)chsum);
    fout->write(&wtchsum, sizeof(uint64_t));
#endif
  }
  // 关闭输出流并释放内存
  fout->close();
  delete fout;
}

} // namespace NativeTask