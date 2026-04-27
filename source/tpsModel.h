#ifndef TPSMODEL_H
#define TPSMODEL_H

#include <map>
#include "common.h"

using namespace std;

class throughput_model{

    public:

        vector<string> loadLines;

        int internalTxLoad = 1;
        double crossShardTxLoad = 1.2;

        double order_capacity = 5000;
        double process_capacity = 8000;

        double total_Throughput = 0; // 系统整体吞吐
        double average_Latency = 0; // 系统平均交易延迟

        map<int, SimulationShard> shards;
        vector<cross_shard_workload> cross_shard_workloads; // 所有的跨片负载
        map<string, map<string, int>> cross_traffic;

    public:

        // 构造函数
        throughput_model(){};

        throughput_model(vector<string> topologyLines);

        // 解析一行字符串中的所有数字，并返回一个整数向量
        vector<int> parseLine(const string& line);

        // 找到两个叶子分片的最近公共祖先
        int findLCA(int shard1, int shard2);

        // 解析拓扑信息
        void parseTopology(const vector<string>& topologyLines);

        // 输出拓扑信息
        void printTopology();

        // 解析负载信息
        void parseLoad();

        // 解析扁平架构下的负载信息
        void parseFlatenLoad(const vector<string>& loadLines);

        // 计算非叶子分片能够处理的交易数量
        int calculateNonLeafTxCount(SimulationShard& shard);

        // 计算叶子分片的吞吐量
        int calculateLeafThroughput(SimulationShard& shard, map<double, double> availableTxs);

        // 计算交易平均延时
        double calculate_average_latency();

        // // 计算系统整体吞吐
        double calculate_total_tps();

        // 解析交易数据并填充 trafficMatrix
        void parseTrafficData(const std::vector<std::string>& transactions);

        // 打印交通矩阵
        void printWorkload();

        // // 获取源分片到目标分片的交易频率
        int getTraffic(const std::string& shard1, const std::string& shard2);
};



#endif // TPSMODEL_H