#include "tpsModel.h"
#include <regex>
#include <sstream>
#include <cstdio>
#include <algorithm>
#include <iostream>

vector<int> throughput_model:: parseLine(const string& line){

    vector<int> numbers;
    // 匹配整数的正则表达式
    regex re("-?\\d+");
    auto begin = sregex_iterator(line.begin(), line.end(), re);
    auto end = sregex_iterator();
    
    for (auto i = begin; i != end; ++i) {
        smatch match = *i;
        int num = stoi(match.str());
        numbers.push_back(num);
    }

    return numbers;
}

int throughput_model::findLCA(int shard1, int shard2){
    set<int> ancestors;
    while (shard1 != -1) {
        ancestors.insert(shard1);
        shard1 = shards[shard1].parent;
    }
    while (shard2 != -1) {
        if (ancestors.count(shard2)) return shard2; // 找到最近公共祖先
        shard2 = shards[shard2].parent;
    }
    return -1; // 不可能发生
}

void throughput_model::parseTopology(const vector<string>& topologyLines){

    shards.clear(); // 先清空shard

    for (auto line : topologyLines){

        vector<int> numbers = parseLine(line);

        int number_size = numbers.size();
        int parentId = numbers.at(0);

        if (shards.find(parentId) == shards.end()) {  // 添加父亲分片
            SimulationShard parentShard;
            parentShard.id = parentId;
            shards[parentId] = parentShard;
        }

        for(int i = 1; i < number_size; i++){ // 添加孩子分片
            int childId = numbers.at(i);

            if (shards.find(childId) == shards.end()) {
                SimulationShard childShard;
                childShard.id = childId;
                shards[childId] = childShard;
            }
            shards[parentId].children.push_back(childId);
            shards[childId].parent = parentId; // 设置子节点的父节点
        }
    }

    // 为顶层分片设置 parent = -1
    for (auto& entry : shards) {
        if (entry.second.parent == -1 && entry.second.children.empty() == false) {
            // 如果一个分片既有子节点又没有父节点，它是顶层分片
            entry.second.parent = -1;
        }
    }
}

void throughput_model::printTopology() {
    // 打印网络拓扑
    for(auto iter = shards.begin(); iter != shards.end(); iter++){
        int children_size = iter->second.children.size();
        if(children_size != 0){
            cout << "分片" << iter->first << "的孩子分片包括:" << endl;
            auto children = iter->second.children;
            for(int i = 0; i < children.size(); i++){
                cout << children.at(i) << " ";
            }
            cout << endl;
        }
    }
}

void throughput_model::parseLoad(){

    regex shardRegex(R"(shard(\d+)_inner:(\d+))");
    for (const string& line : loadLines) {
        smatch match;
        if (regex_search(line, match, shardRegex)) { // 分片的片内负载
            int shardId = stoi(match[1].str());
            int txCount = stoi(match[2].str());
            shards[shardId].internalTxCount = txCount;
        } else if (line.find("shard") != string::npos) {
            int shard1, shard2, txCount;
            sscanf(line.c_str(), "shard%d_shard%d:%d", &shard1, &shard2, &txCount);

            int lca = findLCA(shard1, shard2); // 找到最近公共祖先
            shards[lca].crossShardTxCount += txCount; // 将跨片交易负载分配到LCA

            // cout << "shard1 = " << shard1 << ", shard2 = " << shard2 << ", lca = " << lca << endl;

            cross_shard_workload workload1;
            workload1.non_leaf_shardid = lca;
            workload1.leaf_shardid = shard1;
            workload1.txnum = txCount;
            cross_shard_workloads.push_back(workload1);

            cross_shard_workload workload2;
            workload2.non_leaf_shardid = lca;
            workload2.leaf_shardid = shard2;
            workload2.txnum = txCount;
            cross_shard_workloads.push_back(workload2);
        }
    }

    for (auto& entry : shards) {
        entry.second.process_capacity = process_capacity;
        entry.second.order_capacity = order_capacity;
        // cout << "分片" << entry.first << " 跨片排序负载为" << entry.second.crossShardTxCount << endl;
    }
}

void throughput_model::parseFlatenLoad(const vector<string>& loadLines) {

    regex shardRegex(R"(shard(\d+)_inner:(\d+))");
    for (const string& line : loadLines) {
        smatch match;
        if (regex_search(line, match, shardRegex)) { // 分片的片内负载
            int shardId = stoi(match[1].str());
            int txCount = stoi(match[2].str());
            shards[shardId].internalTxCount = txCount;
            shards[shardId].id = shardId;
        } else if (line.find("shard") != string::npos) {
            int shard1, shard2, txCount;
            sscanf(line.c_str(), "shard%d_shard%d:%d", &shard1, &shard2, &txCount); 
            cross_traffic[to_string(shard1)][to_string(shard2)] = txCount;
            cross_traffic[to_string(shard2)][to_string(shard1)] = txCount;

            shards[shard1].id = shard1;
            shards[shard2].id = shard2;
        }
    }
}

int throughput_model::calculateNonLeafTxCount(SimulationShard& shard){
    return min(shard.crossShardTxCount, shard.order_capacity);
}

int throughput_model::calculateLeafThroughput(SimulationShard& shard, map<double, double> availableTxs){

    // map<int, int> orderedTxs;
    // orderedTxs.insert(make_pair(1, 0));
    // orderedTxs.insert(make_pair(2, 0));

    int orderedCST = 0;

    int internalTxCount = shard.internalTxCount;

    if(internalTxCount >= order_capacity){  // internalTxCount = 0, order_capacity = 5000
        return order_capacity;
    }
    else{
        int remainingTxCount = order_capacity - internalTxCount; // 还能够处理交易次数
        if(remainingTxCount >= availableTxs.at(1)){
            orderedCST = availableTxs.at(1);
        }
        else{
            orderedCST = remainingTxCount;
        }
    }

    // 计算 orderedTxs 中能够处理的负载
    double remaining_process_capacity = process_capacity - internalTxCount * 1;
    double remaining_workload = orderedCST * crossShardTxLoad; // + orderedTxs.at(2) * crossShardTxLoad;

    if(remaining_process_capacity >= remaining_workload){
        return internalTxCount + orderedCST * 0.5;
    }
    else{
        return internalTxCount + (remaining_process_capacity / remaining_workload) * orderedCST * 0.5;
    }
}

double throughput_model::calculate_average_latency(){

    // 计算所有的交易数目
    int total_sub_tx_num = 0;
    double total_latency = 0;

    for (auto& entry : shards) {
        SimulationShard& shard = entry.second;
        if (shard.children.empty()) {  // 叶子分片
            total_sub_tx_num += shard.internalTxCount;
        }
        else{  // 上层非叶子分片
            total_sub_tx_num += shard.crossShardTxCount * 2;
        }
    }

    for (auto& entry : shards) {
        SimulationShard& shard = entry.second;

        if (shard.children.empty()) {  // 叶子分片
            // 当前分片单位时间内接收到的所有跨片交易
            // double received_cross_Txnum = 0;

            map<int, int> received_cross_Txnums; // 记录所有跨层与非跨层的跨片交易
            received_cross_Txnums.insert(make_pair(1, 0));
            received_cross_Txnums.insert(make_pair(2, 0));

            // 找到所有会给当前分片发送跨片交易的上层分片 id
            int workload_size = cross_shard_workloads.size();
            for(int i = 0; i < workload_size; i++){

                auto cross_shard_workload = cross_shard_workloads.at(i);
                int non_leaf_shard = cross_shard_workload.non_leaf_shardid;
                int leaf_shard = cross_shard_workload.leaf_shardid;

                if(leaf_shard == shard.id){
                    // 如果 non_leaf_shard 分片需要排序的交易数量小于排序能力，那么这些交易全部能够排序完
                    if(shards.at(non_leaf_shard).crossShardTxCount <= shards.at(non_leaf_shard).order_capacity){
                        // received_cross_Txnum += cross_shard_workload.txnum;
                        // 判断该交易是否为跨层交易(检查 leaf_shard 是否为 non_leaf_shard 的child)
                        if(count(shards.at(non_leaf_shard).children.begin(), shards.at(non_leaf_shard).children.end(), leaf_shard)){
                            received_cross_Txnums.at(1) = received_cross_Txnums.at(1) + cross_shard_workload.txnum;
                        }
                        else{
                            received_cross_Txnums.at(2) = received_cross_Txnums.at(2) + cross_shard_workload.txnum;
                        }
                    }
                    else{
                        // received_cross_Txnum += shards.at(non_leaf_shard).order_capacity / shards.at(non_leaf_shard).crossShardTxCount * cross_shard_workload.txnum;
                        // 判断该交易是否为跨层交易(检查 leaf_shard 是否为 non_leaf_shard 的child)
                        if(count(shards.at(non_leaf_shard).children.begin(), shards.at(non_leaf_shard).children.end(), leaf_shard)){
                            received_cross_Txnums.at(1) = received_cross_Txnums.at(1) + shards.at(non_leaf_shard).order_capacity / shards.at(non_leaf_shard).crossShardTxCount * cross_shard_workload.txnum;
                        }
                        else{
                            received_cross_Txnums.at(2) = received_cross_Txnums.at(2) + shards.at(non_leaf_shard).order_capacity / shards.at(non_leaf_shard).crossShardTxCount * cross_shard_workload.txnum;
                        }
                    }
                }
            }


            double received_cross_Txnum = received_cross_Txnums.at(1) + received_cross_Txnums.at(2);

            // 加上片内交易
            double received_total_Txnum = received_cross_Txnum + shard.internalTxCount; // 片内交易加收到的跨片交易
            double remaining_Txnum = received_total_Txnum;
            double stage_num = received_total_Txnum / shard.order_capacity;

            if(stage_num < 1){
                total_latency += received_total_Txnum / shard.order_capacity;
            }
            else{
                for(int i = 1; i <= stage_num; i++){
                    if(remaining_Txnum >= shard.order_capacity){

                        total_latency += shard.order_capacity * i;
                        remaining_Txnum -= shard.order_capacity;
                    }
                    else{
                        total_latency += remaining_Txnum * (i - 1) + (remaining_Txnum / shard.order_capacity);
                    }
                }
            }

            total_latency += received_cross_Txnums.at(1) * 0.01;
            total_latency += received_cross_Txnums.at(2) * 0.05;
        }
        else{  // 上层非叶子分片

            double received_cross_Txnum = shard.crossShardTxCount;
            int remaining_Txnum = received_cross_Txnum;
            int stage_num = received_cross_Txnum / shard.order_capacity;

            if(stage_num < 1){
                total_latency += received_cross_Txnum / shard.order_capacity;
            }
            else{
                for(int i = 1; i <= stage_num; i++){
                    if(remaining_Txnum >= shard.order_capacity){
                        total_latency += shard.order_capacity * i;
                        remaining_Txnum -= shard.order_capacity;
                    }
                    else{
                        total_latency += remaining_Txnum * (i - 1) + (remaining_Txnum / shard.order_capacity);
                    }
                }
            }
        }

        // cout << "average_latency = " << total_latency / total_sub_tx_num << endl;
    }
    return total_latency / total_sub_tx_num;
}

double throughput_model::calculate_total_tps(){

    total_Throughput = 0;

    // // 计算系统总吞吐
    for (auto& entry : shards) {
        SimulationShard& shard = entry.second;
        if (shard.children.empty()) {  // 叶子分片
            
            map<double, double> cross_txs;
            cross_txs.insert(make_pair(1, 0)); // 不跨层跨片交易数量
            
            int workload_size = cross_shard_workloads.size(); // 找到所有会给当前分片发送跨片交易的上层分片 id
            for(int i = 0; i < workload_size; i++){
                auto cross_shard_workload = cross_shard_workloads.at(i);
                int non_leaf_shard = cross_shard_workload.non_leaf_shardid; // 发送跨片交易的非下层分片
                int leaf_shard = cross_shard_workload.leaf_shardid; // 接收跨片交易的下层分片

                if(leaf_shard == shard.id){ // 分片作为其接收者

                    if(shards.count(non_leaf_shard) == 0){ // 暂时分片还未形成
                        continue;
                    }

                    // cout << "分片" << non_leaf_shard << "需要排序的跨片交易数量为" << shards.at(non_leaf_shard).crossShardTxCount << endl;

                    if(shards.at(non_leaf_shard).crossShardTxCount <= shards.at(non_leaf_shard).order_capacity){
                        int orderedTxnum = cross_shard_workload.txnum;
                        cross_txs.at(1) += orderedTxnum;
                    }
                    else{
                        int orderedTxnum = (shards.at(non_leaf_shard).order_capacity / shards.at(non_leaf_shard).crossShardTxCount * cross_shard_workload.txnum);
                        cross_txs.at(1) += orderedTxnum;
                    }
                }
            }

            // cout << "分片"  << shard.id << "收到的跨片交易数目为" << cross_txs.at(1) << endl;

            int throughput = calculateLeafThroughput(shard, cross_txs);
            // cout << "此时分片" << shard.id << "的吞吐为" << throughput << endl;
            total_Throughput += throughput;
        }
    }

    // cout << "系统当前整体吞吐为" << total_Throughput << endl;

    return total_Throughput;

}

void throughput_model::printWorkload(){

    // 输出跨片负载矩阵
    for (const auto& outer : cross_traffic) {
        for (const auto& inner : outer.second) {
            std::cout << outer.first << " - " << inner.first << ": " << inner.second << " transactions" << std::endl;
        }
    }
    // 输出片内负载
    for(auto iter = shards.begin(); iter != shards.end(); iter++){
        cout << "shardid = " << iter->first << ", internal_TxNum = " << iter->second.internalTxCount << endl;
    }

}

int throughput_model::getTraffic(const std::string& shard1, const std::string& shard2){

    if (cross_traffic.find(shard1) != cross_traffic.end() && cross_traffic[shard1].find(shard2) != cross_traffic[shard1].end()) {
        return cross_traffic[shard1][shard2];
    }
    return 0;  // 默认返回0表示没有交易
}


throughput_model::throughput_model(vector<string> topologyLines){
    for (const string& line : topologyLines) {
        istringstream iss(line);
        int parentId;
        char ch;

        iss >> ch;  // 跳过 '('
        iss >> parentId;  // 读取父节点ID
        iss >> ch;  // 跳过 ',' 
        iss >> ch;  // 跳过 '('

        // 创建父分片（若不存在则初始化）
        if (shards.find(parentId) == shards.end()) {
            SimulationShard parentShard;
            parentShard.id = parentId;
            shards[parentId] = parentShard;
        }

        // 读取子节点
        while (iss >> ch) {
            if (ch == ')') break; // 结束
            if (ch == ',') continue; // 跳过逗号

            int childId = ch - '0';
            if (shards.find(childId) == shards.end()) {
                SimulationShard childShard;
                childShard.id = childId;
                shards[childId] = childShard;
            }
            shards[parentId].children.push_back(childId);
            shards[childId].parent = parentId; // 设置子节点的父节点
        }
    }

    // 为顶层分片设置 parent = -1
    for (auto& entry : shards) {
        if (entry.second.parent == -1 && entry.second.children.empty() == false) {
            // 如果一个分片既有子节点又没有父节点，它是顶层分片
            entry.second.parent = -1;
        }
    }
}




