#ifndef SHARDSEXTENSION_H
#define SHARDSEXTENSION_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>
#include <memory>
#include <set>

#include "tpsModel.h"

using namespace std;

class shardsExtension {

    public:
        int added_parentid = 1000; // 新增上层分片的起始值
        int order_capacity = 5000; // 分片的交易排序能力
        int added_shardid = 1000; // 新增的父亲分片id

        unordered_map<int, int> innerLoad; // 每个分片的片内交易
        unordered_map<int, std::unordered_map<int, int>> crossShardLoad; // 不同分片之间的跨片交易数量
        map<int, vector<int>> finallParentChildMaps; //最终拓扑结构

        shared_ptr<throughput_model> tm;

    public:
        shardsExtension(vector<string> topologyLines, vector<string>& _loadLines); // 初始化函数

        vector<int> parseLine(const string& line);

        void parseLoad(vector<string>& loadLines); // 解析负载字符串loadLines，更新片内负载和跨片负载 innerLoad 和 crossShardLoad
        
        void parseTopology(vector<string>& topologyLines, map<int, vector<int>>& parentChildMap); // 解析拓扑函数，根据 topologyLines（只会有一行） 获得 parentChildMap

        pair<int, int> getMaxCrossShardTxPair(vector<int>& remaining_shardids); // 从 remaining_shardids 中寻找一批最频繁发生跨片交易的分片对

        int findMostFrequentShard(vector<int>& children_shardids, vector<int>& remaining_shardids); // 寻找 remaining_shardids 中与 children_shardids 所有分片跨片交易最频繁的分片

        double children_cross_txs_load(vector<int>& children_shardids); // 计算一批分片之间的跨片交易负载

        double calucate_total_tps(map<int, vector<int>>& temporary_parent_children); // 计算特定拓扑和负载下系统的吞吐

        set<int> findSubLeafShardids(map<int, vector<int>> parentChildren); // 寻找每一个倒数第二层的结点数

        vector<string> parentChildMapToStrVec(map<int, vector<int>>& parentChildMap);

        void start_optimize(map<int, vector<int>> parentChildren, int extension_shardid); // 启动优化算法，对一个父亲分片若干个孩子分片的拓扑进行优化

        // 输出拓扑信息
        void printTopology();

};

#endif // SHARDSEXTENSION_H
