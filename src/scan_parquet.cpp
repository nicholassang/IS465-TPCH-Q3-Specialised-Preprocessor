#include "scan_parquet.h"

#include <iostream>
#include <vector>
#include <string>

#include <parquet/api/reader.h>
#include <parquet/api/schema.h>

ParquetTable read_parquet(const std::string& filename,
                          const std::vector<int>& col_indices)
{
    ParquetTable table;

    std::cout << "[INFO] Reading parquet file: " << filename << std::endl;

    std::shared_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::OpenFile(filename, false);
    } catch (const std::exception &e) {
        std::cerr << "[ERROR] Failed to open Parquet file '" << filename << "': "
                  << e.what() << std::endl;
        return table;
    }

    if (!reader) {
        std::cerr << "[ERROR] reader returned null for file " << filename << std::endl;
        return table;
    }

    auto metadata = reader->metadata();
    if (!metadata) {
        std::cerr << "[ERROR] Unable to read metadata from " << filename << std::endl;
        return table;
    }

    int num_row_groups = metadata->num_row_groups();
    std::cout << "[DEBUG] Row groups: " << num_row_groups << std::endl;

    for (int col_index : col_indices)
    {
        auto physical_type =
            metadata->schema()->Column(col_index)->physical_type();

        if (physical_type == parquet::Type::INT64)
        {
            std::vector<int64_t> vec;

            for (int rg = 0; rg < num_row_groups; rg++)
            {
                auto row_group = reader->RowGroup(rg);
                auto column_reader = row_group->Column(col_index);

                auto int_reader =
                    static_cast<parquet::Int64Reader*>(column_reader.get());

                while (int_reader->HasNext())
                {
                    int64_t value;
                    int16_t def, rep;
                    int64_t read;

                    int_reader->ReadBatch(1, &def, &rep, &value, &read);

                    if (read > 0)
                        vec.push_back(value);
                }
            }

            table.int64_cols.push_back(vec);
        }

        else if (physical_type == parquet::Type::INT32)
        {
            std::vector<int32_t> vec;

            for (int rg = 0; rg < num_row_groups; rg++)
            {
                auto row_group = reader->RowGroup(rg);
                auto column_reader = row_group->Column(col_index);

                auto int_reader =
                    static_cast<parquet::Int32Reader*>(column_reader.get());

                while (int_reader->HasNext())
                {
                    int32_t value;
                    int16_t def, rep;
                    int64_t read;

                    int_reader->ReadBatch(1, &def, &rep, &value, &read);

                    if (read > 0)
                        vec.push_back(value);
                }
            }

            table.int32_cols.push_back(vec);
        }

        else if (physical_type == parquet::Type::DOUBLE)
        {
            std::vector<double> vec;

            for (int rg = 0; rg < num_row_groups; rg++)
            {
                auto row_group = reader->RowGroup(rg);
                auto column_reader = row_group->Column(col_index);

                auto dbl_reader =
                    static_cast<parquet::DoubleReader*>(column_reader.get());

                while (dbl_reader->HasNext())
                {
                    double value;
                    int16_t def, rep;
                    int64_t read;

                    dbl_reader->ReadBatch(1, &def, &rep, &value, &read);

                    if (read > 0)
                        vec.push_back(value);
                }
            }

            table.double_cols.push_back(vec);
        }

        else if (physical_type == parquet::Type::BYTE_ARRAY)
        {
            std::vector<std::string> vec;

            for (int rg = 0; rg < num_row_groups; rg++)
            {
                auto row_group = reader->RowGroup(rg);
                auto column_reader = row_group->Column(col_index);

                auto str_reader =
                    static_cast<parquet::ByteArrayReader*>(column_reader.get());

                while (str_reader->HasNext())
                {
                    parquet::ByteArray value;
                    int16_t def, rep;
                    int64_t read;

                    str_reader->ReadBatch(1, &def, &rep, &value, &read);

                    if (read > 0)
                    {
                        vec.emplace_back(
                            reinterpret_cast<const char*>(value.ptr),
                            value.len);
                    }
                }
            }

            table.string_cols.push_back(vec);
        }
    }

    // simple sanity: check that all columns read have the same number of rows
    size_t expected = 0;
    if (!table.int64_cols.empty()) expected = table.int64_cols[0].size();
    else if (!table.int32_cols.empty()) expected = table.int32_cols[0].size();
    else if (!table.double_cols.empty()) expected = table.double_cols[0].size();
    else if (!table.string_cols.empty()) expected = table.string_cols[0].size();

    auto check_vecs = [&](auto &vecs, const char *name) {
        for (size_t i = 0; i < vecs.size(); ++i) {
            if (vecs[i].size() != expected) {
                std::cerr << "[WARN] Column " << name << "[" << i << "] has "
                          << vecs[i].size() << " rows but expected " << expected << "\n";
            }
        }
    };
    check_vecs(table.int64_cols, "int64");
    check_vecs(table.int32_cols, "int32");
    check_vecs(table.double_cols, "double");
    check_vecs(table.string_cols, "string");

    return table;
}