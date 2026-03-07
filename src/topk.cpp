#include "topk.h"
#include <algorithm>

std::vector<Result> topk(const std::vector<Result>& results, size_t k) {
    std::vector<Result> res = results;
    std::sort(res.begin(), res.end(), [](const Result& a, const Result& b){
        return a.revenue > b.revenue;
    });
    if (res.size() > k) res.resize(k);
    return res;
}