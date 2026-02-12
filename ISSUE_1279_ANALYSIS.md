# Issue #1279: Inconsistent Selector Exclude Behavior

## Problem Description

dbt Fusion exhibits inconsistent behavior compared to dbt Core when handling selector definitions with `intersection` and `exclude` blocks. Specifically, when an exclude pattern matches no existing models, dbt Fusion returns zero results, whereas dbt Core correctly returns models matching the other criteria.

### Reproduction Case

**Selector Configuration:**
```yaml
- name: test-exclude
  definition:
    intersection:
      - "path:models/test_exclude/bronze/bronze_*"
      - exclude:
        - "path:models/test_exclude/bronse/no_such_model_*"  # Note typo: "bronse" instead of "bronze"
```

**Expected Behavior (dbt Core v1.11.2):**
- Returns 3 models: bronze_1, bronze_2, bronze_3
- The exclude pattern matches no models, so nothing is excluded

**Actual Behavior (dbt Fusion 2.0.0-preview.110):**
- Returns 0 models
- The entire result set is incorrectly filtered out

## Technical Analysis

### Parser Behavior (Correct)

The selector parser in `/home/user/dbt-fusion/crates/dbt-selector-parser/src/parser.rs` correctly creates the following AST:

```rust
SelectExpression::And([
    SelectExpression::Atom(SelectionCriteria {
        method: Path,
        value: "models/test_exclude/bronze/bronze_*",
        ...
    }),
    SelectExpression::Exclude(Box::new(
        SelectExpression::Atom(SelectionCriteria {
            method: Path,
            value: "models/test_exclude/bronse/no_such_model_*",
            ...
        })
    ))
])
```

This AST structure is correct and represents: "Match nodes that satisfy the path pattern AND are NOT excluded by the exclude pattern".

### Evaluation Logic (Bug Location)

The actual node evaluation/scoring logic is in the internal "dbt-scheduler" module (as noted in `/home/user/dbt-fusion/crates/dbt-common/src/node_selector.rs:30`). The bug is likely in how `SelectExpression::Exclude` is evaluated when the inner pattern matches no nodes.

## Root Cause Hypothesis

The evaluation logic likely has one of these bugs:

### Hypothesis 1: Empty Check on Exclude Pattern
```rust
fn evaluate_and(exprs: &[SelectExpression], all_nodes: &[Node]) -> Vec<Node> {
    let mut candidates = all_nodes.clone();
    for expr in exprs {
        match expr {
            Exclude(inner) => {
                // BUG: Checking if pattern matches ANY node globally
                let matches_any = all_nodes.iter().any(|n| matches(inner, n));
                if !matches_any {
                    return vec![];  // ❌ Returns empty when exclude matches nothing
                }
                candidates.retain(|n| !matches(inner, n));
            }
            _ => {
                candidates.retain(|n| matches(expr, n));
            }
        }
    }
    candidates
}
```

**Fix:** Remove the empty check or treat it correctly:
```rust
Exclude(inner) => {
    // Simply filter out nodes that match the exclude pattern
    // If pattern matches nothing, nothing is filtered out
    candidates.retain(|n| !matches(inner, n));
}
```

### Hypothesis 2: Score Calculation Error
If using a score-based approach (0-100):

```rust
fn evaluate_exclude(expr: &SelectExpression, node: &Node) -> Score {
    let matched_nodes = find_matching_nodes(expr, all_nodes);
    if matched_nodes.is_empty() {
        return 0;  // ❌ BUG: Should return 100 (don't exclude anything)
    }
    // Check if this specific node should be excluded
    if matched_nodes.contains(node) {
        return 0;  // Exclude this node
    } else {
        return 100;  // Don't exclude this node
    }
}
```

**Fix:** When the exclude pattern matches no nodes globally, all nodes should get a high score (not excluded):
```rust
fn evaluate_exclude(expr: &SelectExpression, node: &Node) -> Score {
    if matches(expr, node) {
        return 0;  // Exclude this specific node
    } else {
        return 100;  // Don't exclude this node
    }
}
```

### Hypothesis 3: Set Intersection Logic Error
```rust
fn evaluate_and(exprs: &[SelectExpression]) -> Set<NodeId> {
    let sets: Vec<Set<NodeId>> = exprs.iter().map(evaluate).collect();
    // BUG: If any set is empty (even Exclude), intersection becomes empty
    sets.into_iter().reduce(|a, b| a.intersection(&b)).unwrap_or_default()
}

fn evaluate_exclude(expr: &SelectExpression) -> Set<NodeId> {
    let matched = evaluate(expr);
    // BUG: Returns empty set when nothing matches
    // This causes AND intersection to become empty
    if matched.is_empty() {
        return Set::new();  // ❌ Should return all nodes
    }
    all_nodes.difference(&matched)
}
```

**Fix:** When exclude pattern matches nothing, return all nodes (exclude nothing):
```rust
fn evaluate_exclude(expr: &SelectExpression) -> Set<NodeId> {
    let matched = evaluate(expr);
    all_nodes.difference(&matched)  // When matched is empty, returns all nodes ✓
}
```

## Recommended Solutions

### Solution 1: Fix in Evaluation Logic (Preferred)
**Location:** Internal dbt-scheduler module

**Change:** Ensure that `SelectExpression::Exclude` is evaluated as "remove nodes matching this pattern from the candidate set". If the pattern matches no nodes, the candidate set remains unchanged.

**Implementation:**
- For score-based: `Exclude(pattern)` should give score 100 to nodes that don't match the pattern, score 0 to nodes that do match
- For set-based: `Exclude(pattern)` should return `all_nodes - matched_nodes`, which equals `all_nodes` when `matched_nodes` is empty
- For filter-based: Remove any empty-check shortcuts that return empty results when the exclude pattern matches nothing

### Solution 2: Add Validation in Parser (Defensive)
**Location:** `/home/user/dbt-fusion/crates/dbt-selector-parser/src/parser.rs`

**Change:** Add warning or documentation about exclude behavior, but do NOT change the AST structure.

**Rationale:** The parser is creating the correct AST. Changing it would:
- Violate the selector specification
- Require knowledge of available nodes at parse time (not available)
- Mask the real bug instead of fixing it

### Solution 3: Add Integration Tests
**Location:** Test suite

**Change:** Add comprehensive tests for edge cases:
1. Exclude pattern that matches nothing (issue #1279)
2. Exclude pattern that matches some nodes
3. Exclude pattern that matches all nodes
4. Multiple exclude patterns in intersection
5. Nested excludes

## Test Case

A unit test has been added in `/home/user/dbt-fusion/crates/dbt-selector-parser/src/parser.rs`:

```rust
#[test]
fn test_intersection_with_non_matching_exclude() -> FsResult<()> {
    // Test for issue #1279
    // Verifies that the parser creates the correct AST structure
    // The actual evaluation behavior needs to be tested in the scheduler module
    ...
}
```

This test verifies that the parser creates the correct AST. Additional integration tests are needed to verify the evaluation behavior once the fix is implemented in the internal scheduler code.

## Action Items

1. ✅ Identify the bug location (evaluation logic in internal scheduler)
2. ✅ Add parser test case for AST structure
3. ⏳ Fix the evaluation logic in dbt-scheduler (requires access to internal code)
4. ⏳ Add integration tests with actual node matching
5. ⏳ Verify fix matches dbt Core behavior

## References

- Issue: https://github.com/dbt-labs/dbt-fusion/issues/1279
- Parser code: `/home/user/dbt-fusion/crates/dbt-selector-parser/src/parser.rs`
- Selector AST: `/home/user/dbt-fusion/crates/dbt-common/src/node_selector.rs`
- Test case: `/home/user/dbt-fusion/crates/dbt-selector-parser/src/parser.rs:test_intersection_with_non_matching_exclude`
