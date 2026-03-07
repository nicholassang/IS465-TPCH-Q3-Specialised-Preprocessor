#pragma once
#include <string>
#include <vector>
#include <string_view>

struct ParquetTable {
    std::vector<std::vector<int64_t>> int64_cols;
    std::vector<std::vector<int32_t>> int32_cols;
    std::vector<std::vector<double>> double_cols; 
    std::vector<std::vector<std::string>> string_cols;
};

// Read specific columns from a parquet file.
//
// The caller provides a list of column indices to extract; the reader will
// iterate over those indices in order and append each column's data to the
// corresponding vector in the returned ParquetTable.  Columns are collected by
// type, so all INT64 columns appear first (in the order requested), followed
// by INT32, DOUBLE, and then STRING.  The caller must remember this ordering
// when accessing values later (e.g. if you ask for columns [0,1,2,3] and they
// correspond to INT64,INT64,INT32,INT32 respectively then the first two element
// vectors will live in `int64_cols` and the next two in `int32_cols`).
//
// The function returns an empty table if the file cannot be opened or if no
// columns could be read.
ParquetTable read_parquet(const std::string& filename, const std::vector<int>& col_indices);