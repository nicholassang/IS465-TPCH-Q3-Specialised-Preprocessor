#include "scan_parquet.h"
#include "hash_join.h"
#include "aggregation.h"
#include "topk.h"
#include <iostream>
#include <fstream>
#include "types.h"

int main(int argc, char** argv) {
    std::cout << "START" << std::endl;
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " customer.parquet orders.parquet lineitem.parquet" << std::endl;
        return 1;
    }

    std::cout << "START" << std::endl;

    // Read Parquet files
    auto customer = read_parquet(argv[1], {0,1}); // c_custkey, c_mktsegment
    auto orders   = read_parquet(argv[2], {0,1,2,3}); // o_orderkey,o_custkey,o_orderdate,o_shippriority
    auto lineitem = read_parquet(argv[3], {0,1,2,3}); // l_orderkey,l_extendedprice,l_discount,l_shipdate

    std::cout << "[LOG] Done Reading Parquet " << std::endl;

    // Filter customers first
    std::unordered_set<int64_t> valid_customers;
    filter_customers(customer, valid_customers);

    std::cout << "[LOG] Valid customers count: " << valid_customers.size() << std::endl;

    // Build orders hash table only for valid customers
    std::unordered_map<int64_t, OrderInfo> orders_ht;
    build_orders_hash(orders, valid_customers, orders_ht);

    std::cout << "[LOG] Orders inserted into hash table: " << orders_ht.size()
              << " / " << orders.int64_cols[0].size() << std::endl;

    // Aggregate lineitems for orders in hash table
    std::unordered_map<int64_t, AggResult> agg;
    try {
        aggregate_lineitem(lineitem, orders_ht, agg);
    } catch (const std::exception &e) {
        std::cerr << "[ERROR] " << e.what() << std::endl;
    }

    // Collect results
    auto results = collect_results(agg);

    if (results.empty()) {
        std::cout << "[WARN] No lineitems matched filtered orders!" << std::endl;
    }

    // Select top 10 results
    auto top10 = topk(results, 10);

    // Output CSV
    std::cout << "l_orderkey,revenue,o_orderdate,o_shippriority" << std::endl;
    for (auto& r : top10) {
        std::cout << r.orderkey << ","
                  << r.revenue << ","
                  << r.orderdate << ","
                  << r.shippriority << std::endl;
    }

    std::cout << std::flush;
}