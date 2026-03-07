#include "hash_join.h"
#include <iostream>

void filter_customers(const ParquetTable& customer,
                      std::unordered_set<int64_t>& valid_customers) {
    std::cout << "[DEBUG] Start FILTER: " << std::endl;
    std::cout << "[DEBUG] int64 cols: " << customer.int64_cols.size() << std::endl;
    std::cout << "[DEBUG] string cols: " << customer.string_cols.size() << std::endl;

    if (customer.int64_cols.empty() || customer.string_cols.empty()) {
        std::cerr << "[ERROR] Customer table missing expected columns\n";
        return;
    }

    auto& custkey_col = customer.int64_cols[0];
    auto& mktseg_col  = customer.string_cols[0];

    std::cout << "[DEBUG] custkey size: " << custkey_col.size() << std::endl;
    std::cout << "[DEBUG] mktsegment size: " << mktseg_col.size() << std::endl;

    const std::string TARGET_SEGMENT = "BUILDING";

    size_t n = custkey_col.size();
    if (mktseg_col.size() != n) {
        std::cerr << "[ERROR] customer columns length mismatch: custkey=" << n
                  << ", mktseg=" << mktseg_col.size() << std::endl;
        n = std::min(n, mktseg_col.size());
    }

    for (size_t i = 0; i < n; ++i) {
        if (mktseg_col[i] == TARGET_SEGMENT) {
            valid_customers.insert(custkey_col[i]);
        }
    }

    std::cout << "[LOG] filter_customers: " << valid_customers.size() << " valid customers selected" << std::endl;
}

void build_orders_hash(const ParquetTable& orders,
                       const std::unordered_set<int64_t>& valid_customers,
                       std::unordered_map<int64_t, OrderInfo>& orders_ht) {

    if (orders.int64_cols.size() < 2 || orders.int32_cols.size() < 2) {
        std::cerr << "[ERROR] Orders table missing expected columns\n";
        return;
    }
    auto& o_orderkey_col = orders.int64_cols[0];
    auto& o_custkey_col  = orders.int64_cols[1];
    auto& o_orderdate_col = orders.int32_cols[0];
    auto& o_shippriority_col = orders.int32_cols[1];

    size_t m = o_orderkey_col.size();
    if (o_custkey_col.size() != m || o_orderdate_col.size() != m || o_shippriority_col.size() != m) {
        std::cerr << "[ERROR] orders columns length mismatch: "
                  << "orderkey=" << m << ", custkey=" << o_custkey_col.size()
                  << ", orderdate=" << o_orderdate_col.size()
                  << ", shippriority=" << o_shippriority_col.size() << std::endl;
        m = std::min({m, o_custkey_col.size(), o_orderdate_col.size(), o_shippriority_col.size()});
    }

    for (size_t i = 0; i < m; ++i) {
        int64_t custkey = o_custkey_col[i];
        if (valid_customers.find(custkey) == valid_customers.end()) continue;

        int64_t orderkey = o_orderkey_col[i];
        orders_ht[orderkey] = { o_orderdate_col[i], o_shippriority_col[i] };
    }

    std::cout << "[INFO] Orders hash table built: " << orders_ht.size() << std::endl;
}