#pragma once
#include "types.h"
#include "scan_parquet.h"
#include <unordered_map>

void aggregate_lineitem(const ParquetTable& lineitem,
                        const std::unordered_map<int64_t, OrderInfo>& orders_ht,
                        std::unordered_map<int64_t, AggResult>& agg);

std::vector<Result> collect_results(const std::unordered_map<int64_t, AggResult>& agg);