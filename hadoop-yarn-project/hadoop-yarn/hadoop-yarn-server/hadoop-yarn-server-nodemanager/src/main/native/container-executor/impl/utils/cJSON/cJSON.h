// 这个文件已经全部加上中文注释
/*
  Copyright (c) 2009-2017 Dave Gamble and cJSON contributors

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/

/**
 * @file cJSON.h
 * @brief cJSON轻量级JSON解析库头文件，为YARN NodeManager容器执行器提供JSON解析能力
 */

#ifndef cJSON__h
#define cJSON__h

#ifdef __cplusplus
extern "C"
{
#endif

/* 识别Windows平台，统一定义平台宏 */
#if !defined(__WINDOWS__) && (defined(WIN32) || defined(WIN64) || defined(_MSC_VER) || defined(_WIN32))
#define __WINDOWS__
#endif

#ifdef __WINDOWS__

/* Windows平台符号导出控制逻辑，处理不同调用约定和符号可见性 */

#define CJSON_CDECL __cdecl
#define CJSON_STDCALL __stdcall

/* 默认导出符号，方便直接使用源码编译 */
#if !defined(CJSON_HIDE_SYMBOLS) && !defined(CJSON_IMPORT_SYMBOLS) && !defined(CJSON_EXPORT_SYMBOLS)
#define CJSON_EXPORT_SYMBOLS
#endif

#if defined(CJSON_HIDE_SYMBOLS)
#define CJSON_PUBLIC(type)   type CJSON_STDCALL
#elif defined(CJSON_EXPORT_SYMBOLS)
#define CJSON_PUBLIC(type)   __declspec(dllexport) type CJSON_STDCALL
#elif defined(CJSON_IMPORT_SYMBOLS)
#define CJSON_PUBLIC(type)   __declspec(dllimport) type CJSON_STDCALL
#endif
#else /* !__WINDOWS__ */
/* 非Windows平台，不需要特殊调用约定 */
#define CJSON_CDECL
#define CJSON_STDCALL

/* GCC/SunCC支持符号可见性控制，导出公共符号 */
#if (defined(__GNUC__) || defined(__SUNPRO_CC) || defined (__SUNPRO_C)) && defined(CJSON_API_VISIBILITY)
#define CJSON_PUBLIC(type)   __attribute__((visibility("default"))) type
#else
#define CJSON_PUBLIC(type) type
#endif
#endif

/* cJSON版本号定义 */
#define CJSON_VERSION_MAJOR 1
#define CJSON_VERSION_MINOR 7
#define CJSON_VERSION_PATCH 8

#include <stddef.h>

/* JSON节点类型定义，采用位掩码标识 */
#define cJSON_Invalid (0)       /* 无效类型 */
#define cJSON_False  (1 << 0)   /* 布尔false */
#define cJSON_True   (1 << 1)   /* 布尔true */
#define cJSON_NULL   (1 << 2)   /* null类型 */
#define cJSON_Number (1 << 3)   /* 数字类型 */
#define cJSON_String (1 << 4)   /* 字符串类型 */
#define cJSON_Array  (1 << 5)   /* 数组类型 */
#define cJSON_Object (1 << 6)   /* 对象类型 */
#define cJSON_Raw    (1 << 7)   /* 原始JSON字符串 */

#define cJSON_IsReference 256       /* 标记该节点是引用，不负责释放子节点 */
#define cJSON_StringIsConst 512     /* 标记字符串是常量，不需要释放 */

/* cJSON节点结构体定义 */
typedef struct cJSON
{
    /* 双向链表指针，用于遍历数组/对象中的节点 */
    struct cJSON *next;
    struct cJSON *prev;
    /* 数组或对象的子节点链表头指针 */
    struct cJSON *child;

    /* 当前节点的类型，对应上面的cJSON_*定义 */
    int type;

    /* 字符串/原始JSON的值，当类型是cJSON_String或cJSON_Raw时有效 */
    char *valuestring;
    /* 整型数值，已废弃，请使用cJSON_SetNumberValue */
    int valueint;
    /* 浮点数值，当类型是cJSON_Number时有效 */
    double valuedouble;

    /* 对象节点的键名，当该节点是对象的子节点时有效 */
    char *string;
} cJSON;

/**
 * @brief 自定义内存分配钩子结构体，用于替换cJSON默认的内存管理函数
 */
typedef struct cJSON_Hooks
{
    /* Windows下malloc/free始终使用CDECL调用约定，此处保持一致 */
      void *(CJSON_CDECL *malloc_fn)(size_t sz);
      void (CJSON_CDECL *free_fn)(void *ptr);
} cJSON_Hooks;

typedef int cJSON_bool;

/**
 * @brief JSON嵌套深度限制，防止解析恶意JSON导致栈溢出
 */
#ifndef CJSON_NESTING_LIMIT
#define CJSON_NESTING_LIMIT 1000
#endif

/**
 * @brief 获取cJSON库版本字符串
 * @return 版本字符串
 */
CJSON_PUBLIC(const char*) cJSON_Version(void);

/**
 * @brief 初始化自定义内存分配钩子
 * @param hooks 钩子结构体指针
 */
CJSON_PUBLIC(void) cJSON_InitHooks(cJSON_Hooks* hooks);

/**
 * @brief 解析JSON字符串，生成cJSON节点树
 * @param value 输入JSON字符串
 * @return 根节点指针，解析失败返回NULL
 * @note 调用者需要使用cJSON_Delete释放返回的节点树
 */
CJSON_PUBLIC(cJSON *) cJSON_Parse(const char *value);

/**
 * @brief 带选项的JSON解析，支持获取解析结束位置，要求字符串空终止
 * @param value 输入JSON字符串
 * @param return_parse_end 输出解析结束位置的指针，可为NULL
 * @param require_null_terminated 是否要求输入必须以空字符结尾
 * @return 根节点指针，解析失败返回NULL
 * @note 解析失败时，return_parse_end会指向错误位置，和cJSON_GetErrorPtr()返回值一致
 */
CJSON_PUBLIC(cJSON *) cJSON_ParseWithOpts(const char *value, const char **return_parse_end, cJSON_bool require_null_terminated);

/**
 * @brief 将cJSON节点树格式化为带缩进的JSON字符串
 * @param item 根节点指针
 * @return 格式化后的JSON字符串，需要调用者释放
 */
CJSON_PUBLIC(char *) cJSON_Print(const cJSON *item);

/**
 * @brief 将cJSON节点树格式化为无缩进的紧凑JSON字符串
 * @param item 根节点指针
 * @return 格式化后的JSON字符串，需要调用者释放
 */
CJSON_PUBLIC(char *) cJSON_PrintUnformatted(const cJSON *item);

/**
 * @brief 使用缓冲策略格式化JSON，减少内存重分配次数
 * @param item 根节点指针
 * @param prebuffer 预估最终字符串大小，预估准确可以减少重分配
 * @param fmt 是否格式化缩进，1=格式化，0=不格式化
 * @return 格式化后的JSON字符串，需要调用者释放
 */
CJSON_PUBLIC(char *) cJSON_PrintBuffered(const cJSON *item, int prebuffer, cJSON_bool fmt);

/**
 * @brief 使用用户预分配的缓冲区格式化JSON
 * @param item 根节点指针
 * @param buffer 用户预分配的缓冲区
 * @param length 缓冲区长度
 * @param format 是否格式化缩进，1=格式化，0=不格式化
 * @return 1成功，0失败
 * @note 为了安全，建议比实际需要多分配5字节，cJSON预估大小可能不准确
 */
CJSON_PUBLIC(cJSON_bool) cJSON_PrintPreallocated(cJSON *item, char *buffer, const int length, const cJSON_bool format);

/**
 * @brief 释放cJSON节点树及所有子节点
 * @param c 根节点指针
 */
CJSON_PUBLIC(void) cJSON_Delete(cJSON *c);

/**
 * @brief 获取数组或对象包含的节点数量
 * @param array 数组/对象节点指针
 * @return 节点数量
 */
CJSON_PUBLIC(int) cJSON_GetArraySize(const cJSON *array);

/**
 * @brief 根据索引获取数组中的节点
 * @param array 数组节点指针
 * @param index 节点索引
 * @return 节点指针，索引越界返回NULL
 */
CJSON_PUBLIC(cJSON *) cJSON_GetArrayItem(const cJSON *array, int index);

/**
 * @brief 根据键名获取对象中的节点，不区分大小写
 * @param object 对象节点指针
 * @param string 键名
 * @return 节点指针，未找到返回NULL
 */
CJSON_PUBLIC(cJSON *) cJSON_GetObjectItem(const cJSON * const object, const char * const string);

/**
 * @brief 根据键名获取对象中的节点，区分大小写
 * @param object 对象节点指针
 * @param string 键名
 * @return 节点指针，未找到返回NULL
 */
CJSON_PUBLIC(cJSON *) cJSON_GetObjectItemCaseSensitive(const cJSON * const object, const char * const string);

/**
 * @brief 检查对象中是否存在指定键名的节点
 * @param object 对象节点指针
 * @param string 键名
 * @return 1存在，0不存在
 */
CJSON_PUBLIC(cJSON_bool) cJSON_HasObjectItem(const cJSON *object, const char *string);

/**
 * @brief 获取解析错误位置的指针
 * @return 错误位置指针，解析成功返回NULL
 */
CJSON_PUBLIC(const char *) cJSON_GetErrorPtr(void);

/**
 * @brief 获取字符串节点的值
 * @param item 字符串节点指针
 * @return 字符串值，非字符串节点返回NULL
 */
CJSON_PUBLIC(char *) cJSON_GetStringValue(cJSON *item);

/* 以下函数用于检查节点类型 */

/**
 * @brief 检查节点是否为无效类型
 * @param item 节点指针
 * @return 1是无效类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsInvalid(const cJSON * const item);

/**
 * @brief 检查节点是否为false
 * @param item 节点指针
 * @return 1是false，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsFalse(const cJSON * const item);

/**
 * @brief 检查节点是否为true
 * @param item 节点指针
 * @return 1是true，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsTrue(const cJSON * const item);

/**
 * @brief 检查节点是否为布尔类型(true/false)
 * @param item 节点指针
 * @return 1是布尔类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsBool(const cJSON * const item);

/**
 * @brief 检查节点是否为null类型
 * @param item 节点指针
 * @return 1是null类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsNull(const cJSON * const item);

/**
 * @brief 检查节点是否为数字类型
 * @param item 节点指针
 * @return 1是数字类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsNumber(const cJSON * const item);

/**
 * @brief 检查节点是否为字符串类型
 * @param item 节点指针
 * @return 1是字符串类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsString(const cJSON * const item);

/**
 * @brief 检查节点是否为数组类型
 * @param item 节点指针
 * @return 1是数组类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsArray(const cJSON * const item);

/**
 * @brief 检查节点是否为对象类型
 * @param item 节点指针
 * @return 1是对象类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsObject(const cJSON * const item);

/**
 * @brief 检查节点是否为原始JSON类型
 * @param item 节点指针
 * @return 1是原始JSON类型，0不是
 */
CJSON_PUBLIC(cJSON_bool) cJSON_IsRaw(const cJSON * const item);

/* 以下函数用于创建不同类型的cJSON节点 */

/**
 * @brief 创建null类型节点
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateNull(void);

/**
 * @brief 创建true类型节点
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateTrue(void);

/**
 * @brief 创建false类型节点
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateFalse(void);

/**
 * @brief 创建布尔类型节点
 * @param boolean 布尔值
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateBool(cJSON_bool boolean);

/**
 * @brief 创建数字类型节点
 * @param num 数字值
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateNumber(double num);

/**
 * @brief 创建字符串类型节点
 * @param string 字符串值
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateString(const char *string);

/**
 * @brief 创建原始JSON类型节点
 * @param raw 原始JSON字符串
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateRaw(const char *raw);

/**
 * @brief 创建空数组节点
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateArray(void);

/**
 * @brief 创建空对象节点
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateObject(void);

/**
 * @brief 创建字符串引用节点，字符串由调用者管理，cJSON不会释放它
 * @param string 字符串指针
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateStringReference(const char *string);

/**
 * @brief 创建对象引用节点，子节点由调用者管理，cJSON不会释放它
 * @param child 子节点指针
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateObjectReference(const cJSON *child);

/**
 * @brief 创建数组引用节点，子节点由调用者管理，cJSON不会释放它
 * @param child 子节点指针
 * @return 新节点指针
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateArrayReference(const cJSON *child);

/* 以下函数根据C数组批量创建JSON数组 */

/**
 * @brief 根据整型C数组创建JSON数组
 * @param numbers 整型数组指针
 * @param count 数组长度
 * @return 新JSON数组节点
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateIntArray(const int *numbers, int count);

/**
 * @brief 根据单精度浮点型C数组创建JSON数组
 * @param numbers 浮点数组指针
 * @param count 数组长度
 * @return 新JSON数组节点
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateFloatArray(const float *numbers, int count);

/**
 * @brief 根据双精度浮点型C数组创建JSON数组
 * @param numbers 双精度数组指针
 * @param count 数组长度
 * @return 新JSON数组节点
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateDoubleArray(const double *numbers, int count);

/**
 * @brief 根据字符串C数组创建JSON数组
 * @param strings 字符串指针数组
 * @param count 数组长度
 * @return 新JSON数组节点
 */
CJSON_PUBLIC(cJSON *) cJSON_CreateStringArray(const char **strings, int count);

/* 以下函数用于向数组/对象添加节点 */

/**
 * @brief 向数组末尾添加节点
 * @param array 数组节点指针
 * @param item 要添加的节点指针
 */
CJSON_PUBLIC(void) cJSON_AddItemToArray(cJSON *array, cJSON *item);

/**
 * @brief 向对象添加节点
 * @param object 对象节点指针
 * @param string 节点键名
 * @param item 要添加的节点指针
 */
CJSON_PUBLIC(void) cJSON_AddItemToObject(cJSON *object, const char *string, cJSON *item);

/**
 * @brief 向对象添加节点，键名是常量，不拷贝
 * @param object 对象节点指针
 * @param string 常量键名
 * @param item 要添加的节点指针
 * @note 使用此函数后，修改item->string前必须检查(item->type & cJSON_StringIsConst)是否为0
 */
CJSON_PUBLIC(void) cJSON_AddItemToObjectCS(cJSON *object, const char *string, cJSON *item);

/**
 * @brief 向数组添加引用节点，不影响原节点链表结构
 * @param array 数组节点指针
 * @param item 要添加的节点指针
 * @note 当需要将已有节点添加到新结构