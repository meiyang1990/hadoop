// 这个文件已经全部加上中文注释
/*
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
 * @file primitives.h
 * @brief Hadoop MapReduce本地任务原生基础原语函数头文件
 *
 * 提供高性能的内存拷贝、字节序交换、内存比较、内存相等判断等基础原语，
 * 针对MapReduce本地任务的短内存操作场景做了性能优化，用于提升NativeTask的执行效率
 */

#ifndef PRIMITIVES_H_
#define PRIMITIVES_H_

#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <string>

// 分支预测宏，帮助编译器做分支优化
#ifdef __GNUC__
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

//#define SIMPLE_MEMCPY

#if !defined(SIMPLE_MEMCPY)
// 未启用自定义优化版memcpy时，直接使用标准库memcpy
#define simple_memcpy memcpy
#define simple_memcpy2 memcpy
#else

/**
 * 针对小于64字节的小内存拷贝优化的自定义memcpy实现
 * 假定源和目的内存区域不重叠，针对x86-64平台未对齐64位访问做了优化
 *
 * @param dest 目标缓冲区指针
 * @param src 源缓冲区指针
 * @param len 需要拷贝的字节长度，必须大于0
 */
inline void simple_memcpy(void * dest, const void * src, size_t len) {
  const uint8_t * src8 = (const uint8_t*)src;
  uint8_t * dest8 = (uint8_t*)dest;
  switch (len) {
    case 0:
    return;
    case 1:
    dest8[0]=src8[0];
    return;
    case 2:
    *(uint16_t*)dest8=*(const uint16_t*)src8;
    return;
    case 3:
    *(uint16_t*)dest8 = *(const uint16_t*)src8;
    dest8[2]=src8[2];
    return;
    case 4:
    *(uint32_t*)dest8 = *(const uint32_t*)src8;
    return;
  }
  // 长度在5-7字节之间：拷贝首尾4字节覆盖所有数据
  if (len<8) {
    *(uint32_t*)dest8 = *(const uint32_t*)src8;
    *(uint32_t*)(dest8+len-4) = *(const uint32_t*)(src8+len-4);
    return;
  }
  // 长度在8-127字节之间：按8字节块从后往前拷贝
  if (len<128) {
    int64_t cur = (int64_t)len - 8;
    while (cur>0) {
      *(uint64_t*)(dest8+cur) = *(const uint64_t*)(src8+cur);
      cur -= 8;
    }
    *(uint64_t*)(dest8) = *(const uint64_t*)(src8);
    return;
  }
  // 长度超过128字节：退回到标准memcpy
  ::memcpy(dest, src, len);
}

#endif

/**
 * 32位数值字节序交换：小端转大端或大端转小端
 * 根据不同CPU架构使用对应最优实现
 *
 * @param val 需要交换字节序的32位数值
 * @return 交换字节序后的32位数值
 */
inline uint32_t bswap(uint32_t val) {
#ifdef __aarch64__
  // ARM64架构使用汇编指令rev实现
  __asm__("rev %w[dst], %w[src]" : [dst]"=r"(val) : [src]"r"(val));
#elif defined(__ppc64__)||(__PPC64__)||(__powerpc64__)||(__loongarch64)||(__riscv)
  // 特定架构使用GCC内置函数实现
  return  __builtin_bswap32(val);
#else
  // x86架构使用汇编指令bswap实现
  __asm__("bswap %0" : "=r" (val) : "0" (val));
#endif
  return val;
}

/**
 * 64位数值字节序交换：小端转大端或大端转小端
 * 根据不同CPU架构使用对应最优实现
 *
 * @param val 需要交换字节序的64位数值
 * @return 交换字节序后的64位数值
 */
inline uint64_t bswap64(uint64_t val) {
#ifdef __aarch64__
  __asm__("rev %[dst], %[src]" : [dst]"=r"(val) : [src]"r"(val));
#elif defined(__ppc64__)||(__PPC64__)||(__powerpc64__)||(__loongarch64)||(__riscv)
  return __builtin_bswap64(val);
#else
#ifdef __X64
  // X64架构使用汇编指令bswapq实现
  __asm__("bswapq %0" : "=r" (val) : "0" (val));
#else
  // 通用架构：拆分为两个32位分别交换后合并
  uint64_t lower = val & 0xffffffffU;
  uint32_t higher = (val >> 32) & 0xffffffffU;

  lower = bswap(lower);
  higher = bswap(higher);

  return (lower << 32) + higher;

#endif
#endif
  return val;
}

/**
 * 高性能内存比较函数，针对MapReduce中键值比较场景优化
 * 先按8字节块批量比较，发现差异后立即返回，减少比较次数
 *
 * @param src 第一个内存块指针
 * @param dest 第二个内存块指针
 * @param len 需要比较的字节长度
 * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
 */
inline int64_t fmemcmp(const char * src, const char * dest, uint32_t len) {

#ifdef BUILDIN_MEMCMP
  // 启用标准库memcmp时直接调用
  return memcmp(src, dest, len);
#else
  // 小长度特殊处理，逐字节/按字直接比较
  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return 0;
  case 1:
    return (int64_t)src8[0] - (int64_t)dest8[0];
  case 2: {
    int64_t ret = ((int64_t)src8[0] - (int64_t)dest8[0]);
    if (ret)
      return ret;
    return ((int64_t)src8[1] - (int64_t)dest8[1]);
  }
  case 3: {
    int64_t ret = ((int64_t)src8[0] - (int64_t)dest8[0]);
    if (ret)
      return ret;
    ret = ((int64_t)src8[1] - (int64_t)dest8[1]);
    if (ret)
      return ret;
    return ((int64_t)src8[2] - (int64_t)dest8[2]);
  }
  case 4: {
    // 交换字节序后比较大端格式数值
    return (int64_t)bswap(*(uint32_t*)src) - (int64_t)bswap(*(uint32_t*)dest);
  }
  }
  // 长度在5-7字节之间：比较首尾4字节
  if (len < 8) {
    int64_t ret = ((int64_t)bswap(*(uint32_t*)src) - (int64_t)bswap(*(uint32_t*)dest));
    if (ret) {
      return ret;
    }
    return ((int64_t)bswap(*(uint32_t*)(src + len - 4))
        - (int64_t)bswap(*(uint32_t*)(dest + len - 4)));
  }
  // 长度大于等于8字节：按8字节块批量比较
  uint32_t cur = 0;
  uint32_t end = len & (0xffffffffU << 3);
  while (cur < end) {
    uint64_t l = *(uint64_t*)(src8 + cur);
    uint64_t r = *(uint64_t*)(dest8 + cur);
    if (l != r) {
      // 发现差异，交换字节序后比较大小
      l = bswap64(l);
      r = bswap64(r);
      return l > r ? 1 : -1;
    }
    cur += 8;
  }
  // 比较最后剩余的8字节，覆盖所有剩余位
  uint64_t l = *(uint64_t*)(src8 + len - 8);
  uint64_t r = *(uint64_t*)(dest8 + len - 8);
  if (l != r) {
    l = bswap64(l);
    r = bswap64(r);
    return l > r ? 1 : -1;
  }
  // 所有位都相等
  return 0;
#endif
}

/**
 * 针对不同长度内存块的高性能比较函数
 * 先比较公共前缀长度，前缀相等则比较总长度判断大小
 *
 * @param src 第一个内存块指针
 * @param dest 第二个内存块指针
 * @param srcLen 第一个内存块长度
 * @param destLen 第二个内存块长度
 * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
 */
inline int64_t fmemcmp(const char * src, const char * dest, uint32_t srcLen, uint32_t destLen) {
  uint32_t minlen = srcLen < destLen ? srcLen : destLen;
  int64_t ret = fmemcmp(src, dest, minlen);
  if (ret) {
    return ret;
  }
  // 公共前缀相等，根据长度判断大小
  return (int64_t)srcLen - (int64_t)destLen;
}

/**
 * 高性能内存相等判断函数，相比fmemcmp只需要返回布尔结果，性能更优
 * 针对MapReduce中键相等判断场景优化
 *
 * @param src 第一个内存块指针
 * @param dest 第二个内存块指针
 * @param len 需要比较的字节长度
 * @return true表示两块内存内容完全相等，false表示不相等
 */
inline bool fmemeq(const char * src, const char * dest, uint32_t len) {
#ifdef BUILDIN_MEMCMP
  return 0 == memcmp(src, dest, len);
#else
  // 小长度特殊处理
  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return true;
  case 1:
    return src8[0] == dest8[0];
  case 2:
    return *(uint16_t*)src8 == *(uint16_t*)dest8;
  case 3:
    return (*(uint16_t*)src8 == *(uint16_t*)dest8) && (src8[2] == dest8[2]);
  case 4:
    return *(uint32_t*)src8 == *(uint32_t*)dest8;
  }
  // 长度在5-7字节之间：比较首尾4字节
  if (len < 8) {
    return (*(uint32_t*)src8 == *(uint32_t*)dest8)
        && (*(uint32_t*)(src8 + len - 4) == *(uint32_t*)(dest8 + len - 4));
  }
  // 长度大于等于8字节：按8字节块批量比较
  uint32_t cur = 0;
  uint32_t end = len & (0xffffffff << 3);
  while (cur < end) {
    uint64_t l = *(uint64_t*)(src8 + cur);
    uint64_t r = *(uint64_t*)(dest8 + cur);
    if (l != r) {
      return false;
    }
    cur += 8;
  }
  // 比较最后8字节覆盖剩余位
  uint64_t l = *(uint64_t*)(src8 + len - 8);
  uint64_t r = *(uint64_t*)(dest8 + len - 8);
  if (l != r) {
    return false;
  }
  // 所有位都相等
  return true;
#endif
}

/**
 * 针对不同长度内存块的高性能相等判断函数
 *
 * @param src 第一个内存块指针
 * @param srcLen 第一个内存块长度
 * @param dest 第二个内存块指针
 * @param destLen 第二个内存块长度
 * @return true表示两块内存长度和内容都完全相等，false表示不相等
 */
inline bool fmemeq(const char * src, uint32_t srcLen, const char * dest, uint32_t destLen) {
  if (srcLen != destLen) {
    return false;
  }
  return fmemeq(src, dest, std::min(srcLen, destLen));
}

/**
 * 反向顺序高性能内存相等判断
 * 从内存末尾开始向前比较，适用于后缀匹配场景，差异通常出现在末尾时可提前退出
 *
 * @param src 第一个内存块指针
 * @param dest 第二个内存块指针
 * @param len 需要比较的字节长度
 * @return true表示两块内存内容完全相等，false表示不相等
 */
inline bool frmemeq(const char * src, const char * dest, uint32_t len) {
  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return true;
  case 1:
    return src8[0] == dest8[0];
  case 2:
    return *(uint16_t*)src8 == *(uint16_t*)dest8;
  case 3:
    return (src8[2] == dest8[2]) && (*(uint16_t*)src8 == *(uint16_t*)dest8);
  case 4:
    return *(uint32_t*)src8 == *(uint32_t*)dest8;
  }
  // 长度在5-7字节之间：先比较尾部4字节再比较头部
  if (len < 8) {
    return (*(uint32_t*)(src8 + len - 4) == *(uint32_t*)(dest8 + len - 4))
        && (*(uint32_t*)src8 == *(uint32_t*)dest8);
  }
  // 长度大于等于8字节：从尾部开始按8字节块向前比较
  int32_t cur = (int32_t)len - 8;
  while (cur > 0) {
    if (*(uint64_t*)(src8 + cur) != *(uint64_t*)(dest8 + cur)) {
      return false;
    }
    cur -= 8;
  }
  // 最后比较头部8字节
  return *(uint64_t*)(src8) == *(uint64_t*)(dest8);
}

/**
 * 针对不同长度内存块的反向顺序高性能相等判断函数
 *
 * @param src 第一个内存块指针
 * @param dest 第二个内存块指针
 * @param srcLen 第一个内存块长度
 * @param destLen 第二个内存块长度
 * @return true表示两块内存长度和内容都完全相等，false表示不相等
 */
inline bool frmemeq(const char * src, const char * dest, uint32_t srcLen, uint32_t destLen) {
  if (srcLen != destLen) {
    return false;
  }
  return frmemeq(src, dest, std::min(srcLen, destLen));
}

#endif /* PRIMITIVES_H_ */