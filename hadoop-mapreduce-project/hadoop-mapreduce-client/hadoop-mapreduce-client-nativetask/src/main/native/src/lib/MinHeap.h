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
 * @file MinHeap.h
 * 最小堆通用实现头文件，为MapReduce本地任务提供排序用的堆结构
 * 用于在Native端实现高效的Top-K归并排序，支持多输入分片的合并排序
 */

#ifndef MIN_HEAP_H_
#define MIN_HEAP_H_

#include "NativeTask.h"
#include "lib/Buffers.h"

/**
 * 维护最小堆性质的下沉操作
 * 从指定节点开始向下调整，保证以该节点为根的子树满足最小堆性质
 * @tparam T 堆元素类型
 * @tparam Compare 比较器类型
 * @param first 堆数组起始指针
 * @param rt 当前调整节点的位置（基于1的索引）
 * @param heap_len 堆的总长度
 * @param Comp 比较器对象，用于判断元素大小关系
 */
template<typename T, typename Compare>
void heapify(T* first, int rt, int heap_len, Compare & Comp) {
  while (rt * 2 <= heap_len) // 当前节点不是叶子节点，继续下沉
  {
    int left = (rt << 1); // 左孩子索引（基于1）
    int right = (rt << 1) + 1; // 右孩子索引（基于1）
    int smallest = rt;
    // 比较左孩子和当前最小节点
    if (Comp(*(first + left - 1), *(first + smallest - 1))) {
      smallest = left;
    }
    // 比较右孩子和当前最小节点（右孩子存在时）
    if (right <= heap_len && Comp(*(first + right - 1), *(first + smallest - 1))) {
      smallest = right;
    }
    // 如果最小节点不是当前节点，交换后继续下沉
    if (smallest != rt) {
      std::swap(*(first + smallest - 1), *(first + rt - 1));
      rt = smallest;
    } else {
      // 已经满足最小堆性质，退出
      break;
    }
  }
}

/**
 * 根据给定数组构建最小堆
 * @tparam T 堆元素类型
 * @tparam Compare 比较器类型
 * @param begin 堆数组起始指针
 * @param end 堆数组结束指针
 * @param Comp 比较器对象
 */
template<typename T, typename Compare>
void makeHeap(T* begin, T* end, Compare & Comp) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    // 从最后一个非叶子节点开始自底向上建堆
    for (uint32_t i = heap_len / 2; i >= 1; i--) {
      heapify(begin, i, heap_len, Comp);
    }
  }
}

/**
 * 弹出堆顶元素，并重新调整剩余元素为最小堆
 * 将最后一个元素放到堆顶，然后下沉调整
 * @tparam T 堆元素类型
 * @tparam Compare 比较器类型
 * @param begin 堆数组起始指针
 * @param end 堆数组结束指针
 * @param Comp 比较器对象
 */
template<typename T, typename Compare>
void popHeap(T* begin, T* end, Compare & Comp) {
  *begin = *(end - 1);
  // 调整[begin, end - 1)区间为最小堆
  heapify(begin, 1, end - begin - 1, Comp);
}

#endif /* HEAP_H_ */