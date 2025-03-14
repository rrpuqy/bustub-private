//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bound_star.h
//
// Identification: src/include/binder/expressions/bound_star.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include "binder/bound_expression.h"
#include "common/exception.h"

namespace bustub {

/**
 * The star in SELECT list, e.g. `SELECT * FROM x`.
 */
class BoundStar : public BoundExpression {
 public:
  BoundStar() : BoundExpression(ExpressionType::STAR) {}

  auto HasAggregation() const -> bool override {
    throw bustub::Exception("`HasAggregation` should not have been called on `BoundStar`.");
  }

  auto ToString() const -> std::string override { return "*"; }
};
}  // namespace bustub
