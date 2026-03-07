#pragma once
#include "types.h"
#include "scan_parquet.h"
#include <unordered_map>
#include <unordered_set>

void filter_customers(const ParquetTable& customer,
                      std::unordered_set<int64_t>& valid_customers);

void build_orders_hash(const ParquetTable& orders,
                       const std::unordered_set<int64_t>& valid_customers,
                       std::unordered_map<int64_t, OrderInfo>& orders_ht);