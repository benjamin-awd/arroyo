//! Environment variable interpolation for config files.
//!
//! Supports the following syntax:
//! - `$VAR` or `${VAR}` - substitute with env var value, error if missing
//! - `${VAR:-default}` - use default if VAR is unset OR empty
//! - `${VAR-default}` - use default only if VAR is unset (empty is OK)
//! - `$$` - escape sequence for literal `$`

use regex::Regex;
use std::env;
use std::sync::LazyLock;

/// Regex pattern for environment variable interpolation.
/// Matches:
/// - `$$` (escape sequence)
/// - `${VAR:-default}` or `${VAR-default}` (with optional default)
/// - `${VAR}` (braced variable)
/// - `$VAR` (unbraced variable)
static ENV_VAR_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        \$\$                           # Escape sequence $$
        |
        \$\{                           # Opening ${
            ([A-Za-z_][A-Za-z0-9_]*)   # Variable name (capture group 1)
            (?:                        # Optional default value group
                (:?-)                  # :- or just - (capture group 2)
                ([^}]*)                # Default value (capture group 3)
            )?
        \}                             # Closing }
        |
        \$([A-Za-z_][A-Za-z0-9_]*)     # Unbraced $VAR (capture group 4)
        ",
    )
    .expect("Invalid regex pattern")
});

/// Result of environment variable interpolation.
#[derive(Debug)]
pub struct InterpolationResult {
    /// The interpolated text.
    pub text: String,
    /// Any errors encountered during interpolation.
    pub errors: Vec<String>,
}

impl InterpolationResult {
    /// Returns true if there were no errors.
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Interpolate environment variables in the given text.
///
/// Returns the interpolated text along with any errors encountered.
/// All errors are accumulated so the user can see all missing variables at once.
pub fn interpolate(input: &str) -> InterpolationResult {
    let mut errors = Vec::new();

    let text = ENV_VAR_PATTERN
        .replace_all(input, |caps: &regex::Captures| {
            let full_match = caps.get(0).unwrap().as_str();

            // Handle escape sequence $$
            if full_match == "$$" {
                return "$".to_string();
            }

            // Get variable name from either braced or unbraced form
            let var_name = caps
                .get(1)
                .or_else(|| caps.get(4))
                .map(|m| m.as_str())
                .unwrap_or("");

            // Get default value syntax (if any)
            let default_syntax = caps.get(2).map(|m| m.as_str());
            let default_value = caps.get(3).map(|m| m.as_str());

            match env::var(var_name) {
                Ok(value) => {
                    // Check for newline injection
                    if value.contains('\n') || value.contains('\r') {
                        errors.push(format!(
                            "environment variable '{}' contains newlines, which is not allowed",
                            var_name
                        ));
                        return full_match.to_string();
                    }

                    // Handle empty value with :- syntax
                    if value.is_empty() && default_syntax == Some(":-") {
                        return default_value.unwrap_or("").to_string();
                    }

                    value
                }
                Err(_) => {
                    // Variable is not set
                    if let Some(default) = default_value {
                        default.to_string()
                    } else {
                        errors.push(format!("environment variable '{}' is not set", var_name));
                        full_match.to_string()
                    }
                }
            }
        })
        .to_string();

    InterpolationResult { text, errors }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn with_env_vars<F, R>(vars: &[(&str, Option<&str>)], f: F) -> R
    where
        F: FnOnce() -> R,
    {
        // Save original values
        let originals: Vec<_> = vars.iter().map(|(k, _)| (*k, env::var(k).ok())).collect();

        // Set test values
        // SAFETY: These tests run serially (not in parallel) and we restore values after
        for (key, value) in vars {
            match value {
                Some(v) => unsafe { env::set_var(key, v) },
                None => unsafe { env::remove_var(key) },
            }
        }

        let result = f();

        // Restore original values
        // SAFETY: Restoring original environment state
        for (key, original) in originals {
            match original {
                Some(v) => unsafe { env::set_var(key, v) },
                None => unsafe { env::remove_var(key) },
            }
        }

        result
    }

    #[test]
    fn test_basic_substitution() {
        with_env_vars(&[("BLIZZARD_TEST_BASIC", Some("hello"))], || {
            let result = interpolate("value: $BLIZZARD_TEST_BASIC");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: hello");
        });
    }

    #[test]
    fn test_braced_substitution() {
        with_env_vars(&[("BLIZZARD_TEST_BRACED", Some("world"))], || {
            let result = interpolate("value: ${BLIZZARD_TEST_BRACED}");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: world");
        });
    }

    #[test]
    fn test_missing_variable_error() {
        with_env_vars(&[("BLIZZARD_TEST_MISSING", None)], || {
            let result = interpolate("value: $BLIZZARD_TEST_MISSING");
            assert!(!result.is_ok());
            assert_eq!(result.errors.len(), 1);
            assert!(result.errors[0].contains("BLIZZARD_TEST_MISSING"));
            assert!(result.errors[0].contains("not set"));
        });
    }

    #[test]
    fn test_multiple_missing_variables() {
        with_env_vars(
            &[("BLIZZARD_TEST_MISS1", None), ("BLIZZARD_TEST_MISS2", None)],
            || {
                let result = interpolate("a: $BLIZZARD_TEST_MISS1, b: $BLIZZARD_TEST_MISS2");
                assert!(!result.is_ok());
                assert_eq!(result.errors.len(), 2);
            },
        );
    }

    #[test]
    fn test_default_value_unset() {
        with_env_vars(&[("BLIZZARD_TEST_UNSET", None)], || {
            let result = interpolate("value: ${BLIZZARD_TEST_UNSET:-default}");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: default");
        });
    }

    #[test]
    fn test_default_value_empty_with_colon() {
        with_env_vars(&[("BLIZZARD_TEST_EMPTY_COLON", Some(""))], || {
            let result = interpolate("value: ${BLIZZARD_TEST_EMPTY_COLON:-default}");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: default");
        });
    }

    #[test]
    fn test_default_value_empty_without_colon() {
        with_env_vars(&[("BLIZZARD_TEST_EMPTY_NOCOLON", Some(""))], || {
            let result = interpolate("value: ${BLIZZARD_TEST_EMPTY_NOCOLON-default}");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: ");
        });
    }

    #[test]
    fn test_default_value_set_variable() {
        with_env_vars(&[("BLIZZARD_TEST_SET", Some("actual"))], || {
            let result = interpolate("value: ${BLIZZARD_TEST_SET:-default}");
            assert!(result.is_ok());
            assert_eq!(result.text, "value: actual");
        });
    }

    #[test]
    fn test_escape_sequence() {
        let result = interpolate("price: $$100");
        assert!(result.is_ok());
        assert_eq!(result.text, "price: $100");
    }

    #[test]
    fn test_newline_injection_blocked() {
        with_env_vars(&[("BLIZZARD_TEST_INJECT_NL", Some("line1\nline2"))], || {
            let result = interpolate("value: $BLIZZARD_TEST_INJECT_NL");
            assert!(!result.is_ok());
            assert!(result.errors[0].contains("newlines"));
        });
    }

    #[test]
    fn test_carriage_return_injection_blocked() {
        with_env_vars(&[("BLIZZARD_TEST_INJECT_CR", Some("line1\rline2"))], || {
            let result = interpolate("value: $BLIZZARD_TEST_INJECT_CR");
            assert!(!result.is_ok());
            assert!(result.errors[0].contains("newlines"));
        });
    }

    #[test]
    fn test_no_interpolation_needed() {
        let result = interpolate("plain text without variables");
        assert!(result.is_ok());
        assert_eq!(result.text, "plain text without variables");
    }

    #[test]
    fn test_mixed_variables() {
        with_env_vars(
            &[
                ("BLIZZARD_TEST_MIX1", Some("first")),
                ("BLIZZARD_TEST_MIX2", Some("second")),
                ("BLIZZARD_TEST_MIX3", None),
            ],
            || {
                let result = interpolate(
                    "a: $BLIZZARD_TEST_MIX1, b: ${BLIZZARD_TEST_MIX2}, c: ${BLIZZARD_TEST_MIX3:-third}",
                );
                assert!(result.is_ok());
                assert_eq!(result.text, "a: first, b: second, c: third");
            },
        );
    }

    #[test]
    fn test_yaml_config_example() {
        with_env_vars(
            &[
                ("BLIZZARD_TEST_S3_BUCKET", Some("my-bucket")),
                ("BLIZZARD_TEST_AWS_KEY", Some("AKIA123")),
                ("BLIZZARD_TEST_AWS_SECRET", Some("secret")),
                ("BLIZZARD_TEST_AWS_REGION", None),
            ],
            || {
                let yaml = r#"
source:
  path: "s3://${BLIZZARD_TEST_S3_BUCKET}/input/*.ndjson.gz"
  storage_options:
    aws_access_key_id: ${BLIZZARD_TEST_AWS_KEY}
    aws_secret_access_key: ${BLIZZARD_TEST_AWS_SECRET}
    aws_region: ${BLIZZARD_TEST_AWS_REGION:-us-east-1}
"#;
                let result = interpolate(yaml);
                assert!(result.is_ok());
                assert!(result.text.contains("s3://my-bucket/input/*.ndjson.gz"));
                assert!(result.text.contains("aws_access_key_id: AKIA123"));
                assert!(result.text.contains("aws_secret_access_key: secret"));
                assert!(result.text.contains("aws_region: us-east-1"));
            },
        );
    }
}
