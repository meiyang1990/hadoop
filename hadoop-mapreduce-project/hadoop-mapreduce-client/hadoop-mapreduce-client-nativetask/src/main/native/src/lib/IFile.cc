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
 * @file IFile.cc
 * @brief MapReduce本地任务IFile格式读写实现
 *
 * IFile是MapReduce用于存储溢出(spill)到磁盘的中间数据的文件格式，
 * 支持压缩和校验和验证，负责Map任务输出的持久化和Reduce任务读取。
 */

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "lib/IFile.h"
#include "lib/Compressions.h"
#include "lib/FileSystem.h"

namespace NativeTask {

///////////////////////////////////////////////////////////

/**
 * @brief IFile读取器构造函数，初始化带校验和的解压读取流
 * @param stream 底层输入流
 * @param spill 溢出文件信息描述
 * @param deleteInputStream 是否在析构时自动释放输入流
 */
IFileReader::IFileReader(InputStream * stream, SingleSpillInfo * spill, bool deleteInputStream)
    :  _stream(stream), _source(NULL), _checksumType(spill->checkSumType), _kType(spill->keyType),
        _vType(spill->valueType), _codec(spill->codec), _segmentIndex(-1), _spillInfo(spill),
        _valuePos(NULL), _valueLen(0), _deleteSourceStream(deleteInputStream) {
  _source = new ChecksumInputStream(_stream, _checksumType);
  _source->setLimit(0);
  _reader.init(128 * 1024, _source, _codec);
}

/**
 * @brief IFileReader析构函数，释放占用的资源
 */
IFileReader::~IFileReader() {

  delete _source;
  _source = NULL;

  if (_deleteSourceStream) {
    delete _stream;
    _stream = NULL;
  }
}

/**
 * @brief 移动到下一个分区（Reduce分片）读取
 * @return true 成功定位到下一个分区，false 所有分区读取完成
 * @throw IOException 文件格式错误或校验和不匹配时抛出异常
 *
 * 验证当前分区的校验和，然后定位到下一个分区的起始位置，
 * 为读取该分区内的所有<key, value>对做准备。
 */
bool IFileReader::nextPartition() {
  if (0 != _source->getLimit()) {
    THROW_EXCEPTION(IOException, "bad ifile segment length");
  }
  if (_segmentIndex >= 0) {
    // 读取并验证分区校验和
    uint32_t chsum = 0;
    if (4 != _stream->readFully(&chsum, 4)) {
      THROW_EXCEPTION(IOException, "read ifile checksum failed");
    }
    uint32_t actual = bswap(chsum);
    uint32_t expect = _source->getChecksum();
    if (actual != expect) {
      THROW_EXCEPTION_EX(IOException, "read ifile checksum not match, actual %x expect %x", actual, expect);
    }
  }
  _segmentIndex++;
  if (_segmentIndex < (int)(_spillInfo->length)) {
    // 计算当前分区在文件中的偏移长度
    int64_t end_pos = (int64_t)_spillInfo->segments[_segmentIndex].realEndOffset;
    if (_segmentIndex > 0) {
      end_pos -= (int64)_spillInfo->segments[_segmentIndex - 1].realEndOffset;
    }
    if (end_pos < 0) {
      THROW_EXCEPTION(IOException, "bad ifile format");
    }
    // 扣除末尾4字节校验和，设置读取范围限制，重置校验和计算
    _source->setLimit(end_pos - 4);
    _source->resetChecksum();
    return true;
  } else {
    return false;
  }
}

///////////////////////////////////////////////////////////

/**
 * @brief IFileWriter工厂方法，根据文件路径创建IFile写入器
 * @param filepath 输出文件路径
 * @param spec Map输出规格描述（包含校验类型、键值类型、压缩信息）
 * @param spilledRecords 溢出记录计数器，用于统计
 * @return 新建的IFileWriter实例
 */
IFileWriter * IFileWriter::create(const std::string & filepath, const MapOutputSpec & spec,
    Counter * spilledRecords) {
  OutputStream * fout = FileSystem::getLocal().create(filepath, true);
  IFileWriter * writer = new IFileWriter(fout, spec.checksumType, spec.keyType, spec.valueType,
      spec.codec, spilledRecords, true);
  return writer;
}

/**
 * @brief IFile写入器构造函数，初始化带校验和的压缩写入流
 * @param stream 底层输出流
 * @param checksumType 校验和类型
 * @param ktype 键类型（Text/Bytes）
 * @param vtype 值类型（Text/Bytes）
 * @param codec 压缩算法名称
 * @param counter 记录计数器，可为NULL
 * @param deleteTargetStream 是否在析构时自动释放输出流
 */
IFileWriter::IFileWriter(OutputStream * stream, ChecksumType checksumType, KeyValueType ktype,
    KeyValueType vtype, const string & codec, Counter * counter, bool deleteTargetStream)
    : _stream(stream), _dest(NULL), _checksumType(checksumType), _kType(ktype), _vType(vtype),
        _codec(codec), _recordCounter(counter), _recordCount(0), _deleteTargetStream(deleteTargetStream) {
  _dest = new ChecksumOutputStream(_stream, _checksumType);
  _appendBuffer.init(128 * 1024, _dest, _codec);
}

/**
 * @brief IFileWriter析构函数，释放占用资源
 */
IFileWriter::~IFileWriter() {
  delete _dest;
  _dest = NULL;

  if (_deleteTargetStream) {
    delete _stream;
    _stream = NULL;
  }
}

/**
 * @brief 开始一个新分区（Reduce分片）写入
 *
 * 新建分区元数据，重置校验和计算，准备接收该分区的键值对。
 */
void IFileWriter::startPartition() {
  _spillFileSegments.push_back(IFileSegment());
  _dest->resetChecksum();
}

/**
 * @brief 结束当前分区写入，写入EOF标记和校验和
 *
 * 刷出缓冲区数据，完成压缩块，写入分区校验和，
 * 保存分区的元数据信息（未压缩偏移、实际文件偏移）。
 */
void IFileWriter::endPartition() {
  // 写入EOF标记(-1 -1)表示分区结束
  char EOFMarker[2] = {-1, -1};
  _appendBuffer.write(EOFMarker, 2);
  _appendBuffer.flush();

  // 如果开启了压缩，完成压缩块并重置压缩流状态
  CompressStream * compressionStream = _appendBuffer.getCompressionStream();
  if (NULL != compressionStream) {
    compressionStream->finish();
    compressionStream->resetState();
  }

  // 获取校验和，字节序转换后写入文件
  uint32_t chsum = _dest->getChecksum();
  chsum = bswap(chsum);
  _stream->write(&chsum, sizeof(chsum));
  _stream->flush();
  // 保存分区元数据偏移信息
  IFileSegment * info = &(_spillFileSegments[_spillFileSegments.size() - 1]);
  info->uncompressedEndOffset = _appendBuffer.getCounter();
  info->realEndOffset = _stream->tell();
}

/**
 * @brief 写入一条<key, value>记录到IFile
 * @param key 键内容指针
 * @param keyLen 键长度
 * @param value 值内容指针
 * @param valueLen 值长度
 *
 * 根据键值类型（Text/Bytes）编码长度信息，然后写入实际键值数据，
 * 更新记录计数器。Text类型使用可变长度编码长度，Bytes类型使用大端4字节编码长度。
 */
void IFileWriter::write(const char * key, uint32_t keyLen, const char * value, uint32_t valueLen) {
  // 计算编码后总长度（长度编码 + 实际数据）
  uint32_t keyBuffLen = keyLen;
  uint32_t valBuffLen = valueLen;
  switch (_kType) {
  case TextType:
    keyBuffLen += WritableUtils::GetVLongSize(keyLen);
    break;
  case BytesType:
    keyBuffLen += 4;
    break;
  default:
    break;
  }

  switch (_vType) {
  case TextType:
    valBuffLen += WritableUtils::GetVLongSize(valueLen);
    break;
  case BytesType:
    valBuffLen += 4;
    break;
  default:
    break;
  }

  // 写入键和值的总长度（可变长度编码）
  _appendBuffer.write_vuint2(keyBuffLen, valBuffLen);

  // 写入键长度，根据类型选择编码方式
  switch (_kType) {
  case TextType:
    _appendBuffer.write_vuint(keyLen);
    break;
  case BytesType:
    _appendBuffer.write_uint32_be(keyLen);
    break;
  default:
    break;
  }

  // 写入键实际内容
  if (keyLen > 0) {
    _appendBuffer.write(key, keyLen);
  }

  // 更新记录计数器
  if (NULL != _recordCounter) {
    _recordCounter->increase();
  }
  _recordCount++;

  // 写入值长度，根据类型选择编码方式
  switch (_vType) {
  case TextType:
    _appendBuffer.write_vuint(valueLen);
    break;
  case BytesType:
    _appendBuffer.write_uint32_be(valueLen);
    break;
  default:
    break;
  }
  // 写入值实际内容
  if (valueLen > 0) {
    _appendBuffer.write(value, valueLen);
  }
}

/**
 * @brief 将vector中的分段信息转换为堆分配的数组
 * @param segments 分段信息vector
 * @return 堆分配的分段数组，调用者负责释放
 */
IFileSegment * IFileWriter::toArray(std::vector<IFileSegment> *segments) {
  IFileSegment * segs = new IFileSegment[segments->size()];
  for (size_t i = 0; i < segments->size(); i++) {
    segs[i] = segments->at(i);
  }
  return segs;
}

/**
 * @brief 获取当前写入完成的溢出文件描述信息
 * @return 新建的SingleSpillInfo实例，包含所有分段信息
 */
SingleSpillInfo * IFileWriter::getSpillInfo() {
  const uint32_t size = _spillFileSegments.size();
  return new SingleSpillInfo(toArray(&_spillFileSegments), size, "", _checksumType, _kType, _vType,
      _codec);
}

/**
 * @brief 获取当前写入的统计信息
 * @param[out] offset 输出最后一个分区的未压缩偏移
 * @param[out] realOffset 输出最后一个分区的实际文件偏移
 * @param[out] recordCount 输出总记录数
 */
void IFileWriter::getStatistics(uint64_t & offset, uint64_t & realOffset, uint64_t & recordCount) {
  if (_spillFileSegments.size() > 0) {
    offset = _spillFileSegments[_spillFileSegments.size() - 1].uncompressedEndOffset;
    realOffset = _spillFileSegments[_spillFileSegments.size() - 1].realEndOffset;
  } else {
    offset = 0;
    realOffset = 0;
  }
  recordCount = _recordCount;
}

} // namespace NativeTask