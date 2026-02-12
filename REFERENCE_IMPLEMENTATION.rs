// Reference Implementation: Selector Evaluation with Fix for Issue #1279
// This is a standalone implementation showing how the evaluation logic should work
// Location: Internal dbt-scheduler module (to be integrated)

use std::collections::BTreeSet;
use dbt_common::node_selector::{SelectExpression, SelectionCriteria, MethodName};

/// Node identifier type
type NodeId = String;

/// Simplified node structure for demonstration
#[derive(Debug, Clone)]
struct Node {
    unique_id: NodeId,
    fqn: Vec<String>,
    tags: Vec<String>,
    resource_type: String,
    path: String,
    package_name: String,
}

impl Node {
    /// Check if this node matches the given selection criteria
    fn matches_criteria(&self, criteria: &SelectionCriteria) -> bool {
        match criteria.method {
            MethodName::Tag => {
                self.tags.iter().any(|t| self.matches_pattern(t, &criteria.value))
            }
            MethodName::Fqn => {
                let fqn_str = self.fqn.join(".");
                self.matches_pattern(&fqn_str, &criteria.value)
            }
            MethodName::Path => {
                self.matches_pattern(&self.path, &criteria.value)
            }
            MethodName::ResourceType => {
                self.resource_type == criteria.value
            }
            MethodName::Package => {
                self.package_name == criteria.value
            }
            // Add other method types as needed
            _ => false,
        }
    }

    /// Pattern matching with wildcard support
    fn matches_pattern(&self, value: &str, pattern: &str) -> bool {
        // Simple glob pattern matching
        // In production, use a proper glob library
        if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                let (prefix, suffix) = (parts[0], parts[1]);
                return value.starts_with(prefix) && value.ends_with(suffix);
            }
        }
        value == pattern
    }
}

/// Evaluate a SelectExpression against a collection of nodes
///
/// This is the CORRECTED implementation that fixes issue #1279
fn evaluate_select_expression(
    expr: &SelectExpression,
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    match expr {
        SelectExpression::Atom(criteria) => {
            evaluate_atom(criteria, all_nodes)
        }

        SelectExpression::And(exprs) => {
            evaluate_and(exprs, all_nodes)
        }

        SelectExpression::Or(exprs) => {
            evaluate_or(exprs, all_nodes)
        }

        SelectExpression::Exclude(inner_expr) => {
            evaluate_exclude(inner_expr, all_nodes)
        }
    }
}

/// Evaluate an atomic selection criteria
fn evaluate_atom(
    criteria: &SelectionCriteria,
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    let mut result = BTreeSet::new();

    for node in all_nodes {
        if node.matches_criteria(criteria) {
            result.insert(node.unique_id.clone());
        }
    }

    // Handle nested exclude within the atom
    if let Some(nested_exclude) = &criteria.exclude {
        let excluded_nodes = evaluate_select_expression(nested_exclude, all_nodes);
        // Remove excluded nodes from result
        result.retain(|id| !excluded_nodes.contains(id));
    }

    result
}

/// Evaluate AND expression (intersection)
///
/// ✓ FIXED: This implementation correctly handles Exclude when pattern matches nothing
fn evaluate_and(
    exprs: &[SelectExpression],
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    if exprs.is_empty() {
        return BTreeSet::new();
    }

    // Start with all nodes from first expression
    let mut result = evaluate_select_expression(&exprs[0], all_nodes);

    // Intersect with each subsequent expression
    for expr in &exprs[1..] {
        let matched = evaluate_select_expression(expr, all_nodes);

        // For Exclude expressions, we want to remove matching nodes
        // For other expressions, we want to keep only matching nodes
        match expr {
            SelectExpression::Exclude(_) => {
                // ✓ FIX: Simply remove nodes that are in the matched set
                // If matched is empty (exclude pattern matches nothing),
                // then nothing gets removed - this is CORRECT!
                result.retain(|id| !matched.contains(id));
            }
            _ => {
                // Regular intersection: keep only nodes in both sets
                result.retain(|id| matched.contains(id));
            }
        }
    }

    result
}

/// Evaluate OR expression (union)
fn evaluate_or(
    exprs: &[SelectExpression],
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    let mut result = BTreeSet::new();

    for expr in exprs {
        let matched = evaluate_select_expression(expr, all_nodes);
        result.extend(matched);
    }

    result
}

/// Evaluate EXCLUDE expression
///
/// ✓ FIXED: Returns the set of nodes that should be REMOVED from candidates
fn evaluate_exclude(
    inner_expr: &SelectExpression,
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    // Simply evaluate the inner expression to get nodes to exclude
    // The caller (evaluate_and) will remove these from the result
    evaluate_select_expression(inner_expr, all_nodes)
}

// ============================================================================
// COMPARISON: OLD BUGGY IMPLEMENTATION vs NEW FIXED IMPLEMENTATION
// ============================================================================

/// ❌ OLD BUGGY IMPLEMENTATION (DO NOT USE)
/// This is what was causing issue #1279
#[allow(dead_code)]
fn evaluate_and_buggy(
    exprs: &[SelectExpression],
    all_nodes: &[Node],
) -> BTreeSet<NodeId> {
    if exprs.is_empty() {
        return BTreeSet::new();
    }

    let mut result = evaluate_select_expression(&exprs[0], all_nodes);

    for expr in &exprs[1..] {
        let matched = evaluate_select_expression(expr, all_nodes);

        match expr {
            SelectExpression::Exclude(_) => {
                // ❌ BUG: This check causes the issue!
                // When exclude pattern matches nothing (matched.is_empty()),
                // this incorrectly returns an empty set
                if matched.is_empty() {
                    return BTreeSet::new();  // ❌ WRONG!
                }
                result.retain(|id| !matched.contains(id));
            }
            _ => {
                result.retain(|id| matched.contains(id));
            }
        }
    }

    result
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> Vec<Node> {
        vec![
            Node {
                unique_id: "model.test.bronze_1".to_string(),
                fqn: vec!["test".to_string(), "bronze_1".to_string()],
                tags: vec!["production".to_string()],
                resource_type: "model".to_string(),
                path: "models/test_exclude/bronze/bronze_1.sql".to_string(),
                package_name: "test".to_string(),
            },
            Node {
                unique_id: "model.test.bronze_2".to_string(),
                fqn: vec!["test".to_string(), "bronze_2".to_string()],
                tags: vec!["production".to_string()],
                resource_type: "model".to_string(),
                path: "models/test_exclude/bronze/bronze_2.sql".to_string(),
                package_name: "test".to_string(),
            },
            Node {
                unique_id: "model.test.bronze_3".to_string(),
                fqn: vec!["test".to_string(), "bronze_3".to_string()],
                tags: vec!["production".to_string(), "deprecated".to_string()],
                resource_type: "model".to_string(),
                path: "models/test_exclude/bronze/bronze_3.sql".to_string(),
                package_name: "test".to_string(),
            },
        ]
    }

    #[test]
    fn test_issue_1279_exclude_matches_nothing() {
        let nodes = create_test_nodes();

        // Create selector: intersection of path pattern and exclude (with typo)
        let expr = SelectExpression::And(vec![
            SelectExpression::Atom(SelectionCriteria::new(
                MethodName::Path,
                vec![],
                "models/test_exclude/bronze/bronze_*".to_string(),
                false,
                None,
                None,
                None,
                None,
            )),
            SelectExpression::Exclude(Box::new(
                SelectExpression::Atom(SelectionCriteria::new(
                    MethodName::Path,
                    vec![],
                    "models/test_exclude/bronse/no_such_*".to_string(), // Typo: bronse
                    false,
                    None,
                    None,
                    None,
                    None,
                ))
            )),
        ]);

        let result = evaluate_select_expression(&expr, &nodes);

        // ✓ EXPECTED: All 3 bronze models should be selected
        // (exclude matches nothing, so nothing is excluded)
        assert_eq!(result.len(), 3);
        assert!(result.contains("model.test.bronze_1"));
        assert!(result.contains("model.test.bronze_2"));
        assert!(result.contains("model.test.bronze_3"));
    }

    #[test]
    fn test_exclude_matches_some_nodes() {
        let nodes = create_test_nodes();

        // Exclude nodes with "deprecated" tag
        let expr = SelectExpression::And(vec![
            SelectExpression::Atom(SelectionCriteria::new(
                MethodName::Tag,
                vec![],
                "production".to_string(),
                false,
                None,
                None,
                None,
                None,
            )),
            SelectExpression::Exclude(Box::new(
                SelectExpression::Atom(SelectionCriteria::new(
                    MethodName::Tag,
                    vec![],
                    "deprecated".to_string(),
                    false,
                    None,
                    None,
                    None,
                    None,
                ))
            )),
        ]);

        let result = evaluate_select_expression(&expr, &nodes);

        // ✓ EXPECTED: Only bronze_1 and bronze_2 (bronze_3 has deprecated tag)
        assert_eq!(result.len(), 2);
        assert!(result.contains("model.test.bronze_1"));
        assert!(result.contains("model.test.bronze_2"));
        assert!(!result.contains("model.test.bronze_3"));
    }

    #[test]
    fn test_exclude_matches_all_nodes() {
        let nodes = create_test_nodes();

        // Exclude all bronze models
        let expr = SelectExpression::And(vec![
            SelectExpression::Atom(SelectionCriteria::new(
                MethodName::Path,
                vec![],
                "models/test_exclude/bronze/*".to_string(),
                false,
                None,
                None,
                None,
                None,
            )),
            SelectExpression::Exclude(Box::new(
                SelectExpression::Atom(SelectionCriteria::new(
                    MethodName::Path,
                    vec![],
                    "models/test_exclude/bronze/*".to_string(),
                    false,
                    None,
                    None,
                    None,
                    None,
                ))
            )),
        ]);

        let result = evaluate_select_expression(&expr, &nodes);

        // ✓ EXPECTED: Empty (all nodes excluded)
        assert_eq!(result.len(), 0);
    }
}
