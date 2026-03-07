#include "aggregation.h"
#include <iostream>

void aggregate_lineitem(const ParquetTable& lineitem,
                        const std::unordered_map<int64_t, OrderInfo>& orders_ht,
                        std::unordered_map<int64_t, AggResult>& agg) {

    if (lineitem.int64_cols.size() < 1 || lineitem.double_cols.size() < 2 || lineitem.int32_cols.size() < 1) {
        std::cerr << "[ERROR] Lineitem missing required columns!" << std::endl;
        return;
    }

    auto& l_orderkey = lineitem.int64_cols[0];
    auto& l_extended = lineitem.double_cols[0];
    auto& l_discount = lineitem.double_cols[1];
    auto& l_shipdate = lineitem.int32_cols[0];

    // ensure all columns have equal length before iterating
    size_t n = l_orderkey.size();
    if (l_extended.size() != n || l_discount.size() != n || l_shipdate.size() != n) {
        std::cerr << "[ERROR] lineitem columns lengths mismatch: "
                  << "keys=" << n << ", ext=" << l_extended.size()
                  << ", disc=" << l_discount.size()
                  << ", ship=" << l_shipdate.size() << std::endl;
        n = std::min({n, l_extended.size(), l_discount.size(), l_shipdate.size()});
    }

    int64_t aggregated_count = 0;
    int skipped_count = 0;

    for (size_t i = 0; i < n; ++i) {
        int64_t orderkey = l_orderkey[i];
        auto it = orders_ht.find(orderkey);
        if (it == orders_ht.end()) {
            skipped_count++;
            continue;
        }

        double revenue = l_extended[i] * (1.0 - l_discount[i]);
        AggResult& r = agg[orderkey];
        r.revenue += revenue;
        r.orderdate = it->second.orderdate;
        r.shippriority = it->second.shippriority;

        aggregated_count++;
    }

    std::cout << "[INFO] Aggregated " << aggregated_count << " lineitems, skipped " << skipped_count << std::endl;
}

std::vector<Result> collect_results(const std::unordered_map<int64_t, AggResult>& agg) {
    std::vector<Result> results;
    results.reserve(agg.size());
    for (auto& [orderkey, r] : agg) {
        results.push_back({ orderkey, r.revenue, r.orderdate, r.shippriority });
    }
    std::cout << "[INFO] Collected " << results.size() << " results" << std::endl;
    return results;
}