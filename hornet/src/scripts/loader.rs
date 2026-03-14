use anyhow::Result;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::{collections::hash_map::DefaultHasher, fs, path::Path};

lazy_static! {
    static ref INCLUDE_REGEX: Regex =
        Regex::new(r#"(?m)^[-]{2,3}[ \t]*@include[ \t]["']+([^; \t\n]*)["'];?[ \t]?"#).unwrap();
}

#[derive(Debug, PartialEq)]
pub enum ScriptLoaderError {
    CircularDependency,
    DuplicateIncludes(String),
    IoError(String),
}

#[derive(Debug, Clone)]
struct ScriptMetadata {
    parent_token: Option<String>,
    path: PathBuf,
    token: String,
    content: String,
    includes: HashSet<String>,
}

impl PartialEq for ScriptMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

pub fn load_redis_script(path: &str) -> Result<redis::Script, ScriptLoaderError> {
    let command = load_script_content(path)?;

    Ok(redis::Script::new(command.as_str()))
}

fn load_script_content(path: &str) -> Result<String, ScriptLoaderError> {
    let path = Path::new(path);
    let mut includes: Vec<ScriptMetadata> = Vec::new();

    let content = match fs::read_to_string(path) {
        core::result::Result::Ok(content) => content,
        core::result::Result::Err(err) => return Err(ScriptLoaderError::IoError(err.to_string())),
    };

    let mut meta = ScriptMetadata {
        parent_token: None,
        path: path.to_path_buf(),
        token: get_path_hash(path),
        content,
        includes: HashSet::new(),
    };

    resolve_dependencies(&mut meta, &mut includes)?;

    for include in includes.iter().rev() {
        meta.content = meta
            .content
            .replacen(&include.token, include.content.as_str(), 1);
        meta.content = meta.content.replace(&include.token, "");
    }

    Ok(meta.content)
}

fn get_path_hash(path: &Path) -> String {
    format!("@@{}", calculate_hash(path.to_str().unwrap().to_string()))
}

fn calculate_hash(t: String) -> String {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish().to_string()
}

fn resolve_dependencies(
    script_meta: &mut ScriptMetadata,
    includes: &mut Vec<ScriptMetadata>,
) -> Result<(), ScriptLoaderError> {
    let script_dir = script_meta.path.parent().unwrap();

    for cap in INCLUDE_REGEX.captures_iter(&script_meta.content.clone()) {
        let (line, [include]) = cap.extract();

        if script_meta.includes.contains(include) {
            return Err(ScriptLoaderError::DuplicateIncludes(include.to_string()));
        }

        script_meta.includes.insert(include.to_string());

        let include_path = if include.ends_with(".lua") {
            script_dir.join(include)
        } else {
            script_dir.join(format!("{include}.lua"))
        };

        let token = get_path_hash(&include_path);

        if let Some(parent_token) = &script_meta.parent_token {
            if *parent_token == token {
                return Err(ScriptLoaderError::CircularDependency);
            }
        }

        let mut include_meta: ScriptMetadata = ScriptMetadata {
            parent_token: Some(script_meta.token.clone()),
            token,
            content: match fs::read_to_string(&include_path) {
                Ok(content) => content,
                Err(err) => return Err(ScriptLoaderError::IoError(err.to_string())),
            },
            path: include_path,
            includes: HashSet::new(),
        };

        resolve_dependencies(&mut include_meta, includes)?;

        script_meta.content = script_meta.content.replace(line, &include_meta.token);

        if !includes.contains(&include_meta) {
            includes.push(include_meta.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_basic_include() {
        let fixture = "./tests/fixtures/scripts/fixture_simple_include.lua";
        let command = load_script_content(fixture);

        assert!(command.is_ok());
    }

    #[test]
    fn removes_include_tag() {
        let fixture = "./tests/fixtures/scripts/fixture_simple_include.lua";
        let script = load_script_content(fixture).unwrap();

        assert!(!script.contains("@include"));
    }

    fn parse_included_files(script: String) -> Vec<String> {
        let left = "--- file:";
        let mut res = vec![];

        for line in script.lines() {
            if line.starts_with(left) {
                res.push(line.replace(left, "").trim().to_string());
            }
        }

        res
    }

    #[test]
    fn interpolates_script_exactly_once() {
        let fixture = "./tests/fixtures/scripts/fixture_duplicate_elimination.lua";
        let script = load_script_content(fixture).unwrap();
        let includes = parse_included_files(script);
        let count = includes.iter().filter(|i| **i == "strings.lua").count();

        assert_eq!(count, 1);
    }

    #[test]
    fn inserts_scripts_in_dependency_order() {
        let fixture = "./tests/fixtures/scripts/fixture_recursive_parent.lua";
        let script = load_script_content(fixture).unwrap();
        let includes = parse_included_files(script);

        let expected = vec![
            "strings.lua",
            "fixture_recursive_great_grandchild.lua",
            "fixture_recursive_grandchild.lua",
            "fixture_recursive_child.lua",
            "fixture_recursive_parent.lua",
        ];

        assert_eq!(includes, expected);
    }

    #[test]
    fn detect_circular_dependencies() {
        let fixture = "./tests/fixtures/scripts/fixture_circular_dependency.lua";
        let script = load_script_content(fixture);

        assert!(script.is_err());
        assert_eq!(script.err().unwrap(), ScriptLoaderError::CircularDependency);
    }

    #[test]
    fn prevent_multiple_includes_of_file_in_single_script() {
        let fixture = "./tests/fixtures/scripts/fixture_duplicate_include.lua";
        let script = load_script_content(fixture);

        assert!(script.is_err());
        assert_eq!(
            script.err().unwrap(),
            ScriptLoaderError::DuplicateIncludes("includes/utils".to_string())
        );
    }
}
