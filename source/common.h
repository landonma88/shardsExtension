#ifndef COMMON_H      // ← 移到最顶部
#define COMMON_H

#include <map>
#include <vector>
#include <unordered_map>
#include <string>
#include <sstream>
#include <set>
#include <iostream>
#include <algorithm>
#include <regex>
// #include "shardsExtension.h"

using namespace std;

// 定义模拟分片结构体
struct SimulationShard {
    int id;                    // 分片ID
    double process_capacity;   // 处理综合交易处理能力
    double order_capacity;        // 交易排序能力
    double internalTxCount;       // 片内交易数量
    double crossShardTxCount;     // 跨片交易数量
    vector<int> children;      // 孩子分片ID
    int parent = -1;           // 父节点ID，默认值为 -1 表示无父节点
};

struct topology_with_tps {
    double throughput; // 相应的吞吐
    map<int, vector<int>> parent_children; // 分片的拓扑结构
};

struct cross_shard_workload {
    int non_leaf_shardid; // 发送跨片交易的上层分片
    int leaf_shardid; // 处理交易的下层分片
    double txnum; // 交易数目
};

#endif // COMMON_H