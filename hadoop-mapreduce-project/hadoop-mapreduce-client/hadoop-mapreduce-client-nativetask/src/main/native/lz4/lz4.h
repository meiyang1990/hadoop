// 这个文件已经全部加上中文注释
/*
 *  LZ4 - Fast LZ compression algorithm
 *  Header File
 *  Copyright (C) 2011-present, Yann Collet.

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
    - LZ4 homepage : http://www.lz4.org
    - LZ4 source repository : https://github.com/lz4/lz4
*/

/**
 * @file lz4.h
 * @brief LZ4无损压缩算法头文件，为Hadoop MapReduce本地任务提供高性能压缩能力
 *
 * LZ4是一种高速无损压缩算法，单核心压缩速度超过500MB/s，可随多核CPU线性扩展，
 * 解码速度可达单核心多GB/s，通常能达到多核系统的内存带宽上限。
 * 本文件提供LZ4块格式的压缩与解压API，支持单次压缩、流式压缩和字典压缩等多种使用方式。
 */

#if defined (__cplusplus)
extern "C" {
#endif

#ifndef LZ4_H_2983827168210
#define LZ4_H_2983827168210

/* --- Dependency --- */
#include <stddef.h>   /* size_t */


/**
  Introduction

  LZ4 is lossless compression algorithm, providing compression speed >500 MB/s per core,
  scalable with multi-cores CPU. It features an extremely fast decoder, with speed in
  multiple GB/s per core, typically reaching RAM speed limits on multi-core systems.

  The LZ4 compression library provides in-memory compression and decompression functions.
  It gives full buffer control to user.
  Compression can be done in:
    - a single step (described as Simple Functions)
    - a single step, reusing a context (described in Advanced Functions)
    - unbounded multiple steps (described as Streaming compression)

  lz4.h generates and decodes LZ4-compressed blocks (doc/lz4_Block_format.md).
  Decompressing such a compressed block requires additional metadata.
  Exact metadata depends on exact decompression function.
  For the typical case of LZ4_decompress_safe(),
  metadata includes block's compressed size, and maximum bound of decompressed size.
  Each application is free to encode and pass such metadata in whichever way it wants.

  lz4.h only handle blocks, it can not generate Frames.

  Blocks are different from Frames (doc/lz4_Frame_format.md).
  Frames bundle both blocks and metadata in a specified manner.
  Embedding metadata is required for compressed data to be self-contained and portable.
  Frame format is delivered through a companion API, declared in lz4frame.h.
  The `lz4` CLI can only manage frames.
*/

/*^***************************************************************
*  导出参数控制
*****************************************************************/
/*
*  LZ4_DLL_EXPORT :
*  构建Windows DLL时启用函数导出
*  LZ4LIB_VISIBILITY :
*  控制库符号可见性
*/
#ifndef LZ4LIB_VISIBILITY
#  if defined(__GNUC__) && (__GNUC__ >= 4)
#    define LZ4LIB_VISIBILITY __attribute__ ((visibility ("default")))
#  else
#    define LZ4LIB_VISIBILITY
#  endif
#endif
#if defined(LZ4_DLL_EXPORT) && (LZ4_DLL_EXPORT==1)
#  define LZ4LIB_API __declspec(dllexport) LZ4LIB_VISIBILITY
#elif defined(LZ4_DLL_IMPORT) && (LZ4_DLL_IMPORT==1)
#  define LZ4LIB_API __declspec(dllimport) LZ4LIB_VISIBILITY /* 虽非必需，但允许生成更好的代码，节省从IAT加载函数指针和间接跳转的开销 */
#else
#  define LZ4LIB_API LZ4LIB_VISIBILITY
#endif

/*------   版本信息   ------*/
// 主版本号，用于不兼容的接口变更
#define LZ4_VERSION_MAJOR    1    /* for breaking interface changes  */
// 次版本号，用于新增向后兼容的能力
#define LZ4_VERSION_MINOR    9    /* for new (non-breaking) interface capabilities */
// 修订号，用于调整、bug修复和开发版本
#define LZ4_VERSION_RELEASE  2    /* for tweaks, bug-fixes, or development */

#define LZ4_VERSION_NUMBER (LZ4_VERSION_MAJOR *100*100 + LZ4_VERSION_MINOR *100 + LZ4_VERSION_RELEASE)

#define LZ4_LIB_VERSION LZ4_VERSION_MAJOR.LZ4_VERSION_MINOR.LZ4_VERSION_RELEASE
#define LZ4_QUOTE(str) #str
#define LZ4_EXPAND_AND_QUOTE(str) LZ4_QUOTE(str)
#define LZ4_VERSION_STRING LZ4_EXPAND_AND_QUOTE(LZ4_LIB_VERSION)

/**
 * @brief 获取库版本号，用于检查DLL版本是否匹配
 * @return 整型版本号 (主版本*10000 + 次版本*100 + 修订号)
 */
LZ4LIB_API int LZ4_versionNumber (void);  /**< library version number; useful to check dll version */

/**
 * @brief 获取库版本字符串，用于检查DLL版本是否匹配
 * @return 版本字符串，格式为"主版本.次版本.修订号"
 */
LZ4LIB_API const char* LZ4_versionString (void);   /**< library version string; useful to check dll version */


/*-************************************
*  调优参数
**************************************/
/*!
 * LZ4_MEMORY_USAGE :
 * 内存使用公式: N -> 2^N 字节 (示例: 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; 等等)
 * 增加内存使用量可以提高压缩率
 * 减少内存使用量可以提升速度，因为可以获得更好的缓存局部性
 * 默认值为14，对应16KB，刚好适配Intel x86 L1缓存
 */
#ifndef LZ4_MEMORY_USAGE
# define LZ4_MEMORY_USAGE 14
#endif


/*-************************************
*  简单压缩解压函数
**************************************/
/*! LZ4_compress_default() :
 *  从src缓冲区压缩srcSize字节数据，写入已分配好的dst缓冲区，dst容量为dstCapacity
 *  当dstCapacity >= LZ4_compressBound(srcSize)时，压缩保证成功，并且运行速度更快，这是推荐配置
 *  如果无法在有限的dst空间内完成压缩，压缩会立即停止，函数返回0，此时dst内容无效
 *      srcSize : 最大支持值为LZ4_MAX_INPUT_SIZE
 *      dstCapacity : dst缓冲区的大小 (必须已经分配好内存)
 *     @return  : 写入dst缓冲区的字节数 (一定 <= dstCapacity)，压缩失败返回0
 * Note : 本函数可防止缓冲区溢出 (绝不会写入dst缓冲区外，也不会读取源缓冲区外)
 */
LZ4LIB_API int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);

/*! LZ4_decompress_safe() :
 *  compressedSize : 压缩块完整的精确大小
 *  dstCapacity : 目标缓冲区的大小 (必须已经分配好内存)，是解压后大小的上限
 * @return : 解压到目标缓冲区的字节数 (一定 <= dstCapacity)
 *           如果目标缓冲区不够大，或者检测到源数据损坏，解码会停止并返回负值
 * Note 1 : 本函数可防范恶意数据包:
 *          绝不会写入dst缓冲区外，也不会读取源缓冲区外，即使压缩块被恶意修改，
 *          解码器会立即停止，并认为压缩块损坏。
 * Note 2 : compressedSize和dstCapacity必须由调用者提供，压缩块本身不包含这些信息
 *          实现可以自由选择任何方式来编码/存储/获取这些元数据。
 *          如果需要将压缩数据和元数据绑定在一起，请参考lz4frame.h中的帧格式API。
 */
LZ4LIB_API int LZ4_decompress_safe (const char* src, char* dst, int compressedSize, int dstCapacity);


/*-************************************
*  高级函数
**************************************/
// 最大输入大小：2^31 - 2^25 = 2113929216字节
#define LZ4_MAX_INPUT_SIZE        0x7E000000   /* 2 113 929 216 bytes */
// 计算最坏情况下（不可压缩数据）压缩后的最大大小
#define LZ4_COMPRESSBOUND(isize)  ((unsigned)(isize) > (unsigned)LZ4_MAX_INPUT_SIZE ? 0 : (isize) + ((isize)/255) + 16)

/*! LZ4_compressBound() :
    计算LZ4压缩在最坏情况（输入数据不可压缩）下的最大输出大小
    主要用于内存分配（确定目标缓冲区大小）。
    宏LZ4_COMPRESSBOUND()也可用于编译期计算（例如栈内存分配）。
    注意当dstCapacity >= LZ4_compressBound(srcSize)时，LZ4_compress_default()压缩速度更快
        inputSize  : 最大支持值为LZ4_MAX_INPUT_SIZE
        return : 最坏情况下的最大输出大小，输入大小不正确（过大或负数）返回0
*/
LZ4LIB_API int LZ4_compressBound(int inputSize);

/*! LZ4_compress_fast() :
    和LZ4_compress_default()功能相同，但允许选择加速因子
    加速因子越大，算法越快，但压缩率越低，这是一个权衡。
    可以精细调优，每增加一个加速因子大约能提升~3%速度。
    加速因子为1时和常规LZ4_compress_default()相同
    小于等于0的值会被替换为默认加速因子（当前为1，详见lz4.c）
*/
LZ4LIB_API int LZ4_compress_fast (const char* src, char* dst, int srcSize, int dstCapacity, int acceleration);


/*! LZ4_compress_fast_extState() :
 *  和LZ4_compress_fast()功能相同，但使用外部分配的内存来保存压缩状态
 *  使用LZ4_sizeofState()获取需要分配的内存大小，
 *  并按8字节边界分配内存（通常使用malloc()），
 *  然后将该缓冲区作为void* state传入压缩函数。
 */
/**
 * @brief 获取压缩状态需要的内存大小
 * @return 状态所需的字节数
 */
LZ4LIB_API int LZ4_sizeofState(void);

/**
 * @brief 使用外部预分配状态进行快速压缩
 * @param[in] state 预分配的状态缓冲区
 * @param[in] src 源数据缓冲区
 * @param[out] dst 目标压缩缓冲区
 * @param[in] srcSize 源数据大小
 * @param[in] dstCapacity 目标缓冲区容量
 * @param[in] acceleration 加速因子
 * @return 压缩后大小，0表示压缩失败
 */
LZ4LIB_API int LZ4_compress_fast_extState (void* state, const char* src, char* dst, int srcSize, int dstCapacity, int acceleration);


/*! LZ4_compress_destSize() :
 *  反转压缩逻辑：从src缓冲区尽可能压缩数据，写入大小至少为targetDestSize的已分配dst缓冲区
 *  如果dst足够大，会压缩整个src内容；否则，会尽可能填充dst缓冲区，压缩尽可能多的src数据
 *  注意：加速参数固定为默认值
 *
 * *srcSizePtr : 会被修改，指示从src读取了多少字节来填充dst，新值一定小于等于输入值
 * @return : 写入dst的字节数 (一定 <= targetDestSize)，压缩失败返回0
*/
LZ4LIB_API int LZ4_compress_destSize (const char* src, char* dst, int* srcSizePtr, int targetDstSize);


/*! LZ4_decompress_safe_partial() :
 *  解压位于src处大小为srcSize的LZ4压缩块，写入大小为dstCapacity的dst目标缓冲区
 *  最多解码targetOutputSize字节，函数在达到目标后停止解码
 *  当只需要块的开头部分数据时，可以提升性能
 *
 * @return : 解码到dst的字节数 (一定 <= dstCapacity)，检测到源数据损坏返回负值
 *
 *  Note : 返回值可以小于targetOutputSize，如果压缩块本身包含的数据更少
 *
 *  Note 2 : 本函数有两个参数targetOutputSize和dstCapacity，要求targetOutputSize <= dstCapacity
 *           它确实会在达到targetOutputSize时停止解码，因此dstCapacity有点冗余
 *           这是因为在本函数的之前版本中，解码操作不能在序列中间打断，
 *           因此无法保证解码恰好停在targetOutputSize，可能会写入更多字节，但最多不超过dstCapacity
 *           因此过去操作需要保留一些余量才能正常工作，现在已经不需要了
 *           但为了不破坏API兼容性，函数仍然保留原有签名
 */
LZ4LIB_API int LZ4_decompress_safe_partial (const char* src, char* dst, int srcSize, int targetOutputSize, int dstCapacity);


/*-*********************************************
*  流式压缩函数
***********************************************/
typedef union LZ4_stream_u LZ4_stream_t;  /* 不完整类型，后续定义 */

/**
 * @brief 创建流式压缩上下文
 * @return 分配好的流式压缩上下文指针，分配失败返回NULL
 */
LZ4LIB_API LZ4_stream_t* LZ4_createStream(void);

/**
 * @brief 释放流式压缩上下文
 * @param[in] streamPtr 要释放的上下文指针
 * @return 0表示释放成功
 */
LZ4LIB_API int           LZ4_freeStream (LZ4_stream_t* streamPtr);

/*! LZ4_resetStream_fast() : v1.9.0+
 *  用于准备LZ4_stream_t，开始新的依赖块链（例如LZ4_compress_fast_continue()）
 *
 *  LZ4_stream_t在使用前必须初始化一次。
 *  通过LZ4_createStream()创建时会自动完成初始化。
 *  但如果LZ4_stream_t是在栈上直接声明的，必须先用LZ4_initStream()初始化。
 *
 *  初始化后，使用LZ4_resetStream_fast()开始任何新流。
 *  同一个LZ4_stream_t可以连续多次复用，压缩多个流，只需要每次新流开始时调用LZ4_resetStream_fast()。
 *
 *  LZ4_resetStream_fast()比LZ4_initStream()快得多，但不适用于包含垃圾数据的内存区域。
 *
 *  注意：只有在流式压缩场景下调用这个函数才有意义，extState函数会自己执行重置，
 *  之前调用LZ4_resetStream_fast()是多余的，甚至会起反作用。
 */
/**
 * @brief 快速重置流式压缩上下文，准备开始新流
 * @param[in] streamPtr 要重置的上下文指针
 */
LZ4LIB_API void LZ4_resetStream_fast (LZ4_stream_t* streamPtr);

/*! LZ4_loadDict() :
 *  使用此函数将静态字典加载到LZ4_stream_t中，字典在压缩期间必须保持可用
 *  LZ4_loadDict()会触发重置，因此之前的所有数据都会被遗忘。
 *  解压侧必须加载相同的字典才能成功解码。
 *  字典对于小数据（KB级别）的压缩率提升效果很好。
 *  虽然LZ4接受任何输入作为字典，但使用Zstandard的字典构建器生成的字典通常能获得更好的结果。
 *  加载大小为0是允许的，效果等同于重置。
 * @return : 加载的字典大小，单位字节（一定 <= 64 KB）
 */
LZ4LIB_API int LZ4_loadDict (LZ4_stream_t* streamPtr, const char* dictionary, int dictSize);

/*! LZ4_compress_fast_continue() :
 *  使用之前压缩块的数据来压缩当前src内容，可以获得更好的压缩率。
 *  dst缓冲区必须已经分配好内存。
 *  如果dstCapacity >= LZ4_compressBound(srcSize)，压缩保证成功，运行更快。
 *
 * @return : 压缩块大小，出错（通常是无法放入dst）返回0
 *