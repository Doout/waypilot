//! Durable enabled-state overrides. Configuration remains the user/grant source.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    users: BTreeMap<String, bool>,
}
pub(crate) struct Policy {
    path: Option<PathBuf>,
    state: State,
    _lock: Option<std::fs::File>,
}
impl Policy {
    pub fn load(path: Option<PathBuf>, ids: &[String]) -> Result<Self> {
        let lock = if let Some(path) = &path {
            let directory = path.parent().context("policy path needs a parent")?;
            std::fs::create_dir_all(directory)?;
            let lock_path = path.with_extension("lock");
            if lock_path.exists() {
                ensure!(
                    std::fs::symlink_metadata(&lock_path)?.file_type().is_file(),
                    "policy lock must be a regular file"
                );
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options.open(lock_path)?;
            file.try_lock()
                .context("policy state already owned or lock unavailable")?;
            Some(file)
        } else {
            None
        };
        let state = if let Some(path) = path.as_ref().filter(|path| path.exists()) {
            ensure!(
                std::fs::symlink_metadata(path)?.file_type().is_file(),
                "policy state must be a regular file"
            );
            ensure!(
                std::fs::metadata(path)?.len() <= 65536,
                "policy state exceeds limit"
            );
            let state: State =
                serde_json::from_slice(&std::fs::read(path)?).context("invalid policy state")?;
            ensure!(
                state.version == 1 && state.users.keys().all(|id| ids.contains(id)),
                "foreign policy state users/version"
            );
            state
        } else {
            State {
                version: 1,
                users: BTreeMap::new(),
            }
        };
        if let Some(path) = &path {
            replace(path, &serde_json::to_vec(&state)?)?;
        }
        Ok(Self {
            path,
            state,
            _lock: lock,
        })
    }
    pub fn enabled(&self, id: &str, default: bool) -> bool {
        self.state.users.get(id).copied().unwrap_or(default)
    }
    pub fn persistent(&self) -> bool {
        self.path.is_some()
    }
    pub fn set(&mut self, id: &str, enabled: bool) -> Result<()> {
        let mut users = self.state.users.clone();
        users.insert(id.into(), enabled);
        if let Some(path) = self.path.clone() {
            let bytes = serde_json::to_vec(&State {
                version: 1,
                users: users.clone(),
            })?;
            replace(&path, &bytes)?;
        }
        self.state.users = users;
        Ok(())
    }
}
fn replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let directory = path.parent().context("policy path needs a parent")?;
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(
        ".waypilot-state-{:032x}.tmp",
        rand::random::<u128>()
    ));
    let result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(directory)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "waypilot-policy-test-{:032x}",
            rand::random::<u128>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
    #[test]
    fn restart_preserves_revocation_and_rejects_foreign_state() {
        let directory = directory();
        let path = directory.join("state.json");
        let users = vec!["alice".into()];
        let mut policy = Policy::load(Some(path.clone()), &users).unwrap();
        policy.set("alice", false).unwrap();
        assert!(
            Policy::load(Some(path.clone()), &users).is_err(),
            "second writer acquired live policy state"
        );
        drop(policy);
        assert!(!Policy::load(Some(path.clone()), &users)
            .unwrap()
            .enabled("alice", true));
        assert!(Policy::load(Some(path.clone()), &["bob".into()]).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn failed_write_does_not_publish_a_new_override() {
        let directory = directory();
        let parent = directory.join("state");
        let mut policy = Policy::load(Some(parent.join("state.json")), &["alice".into()]).unwrap();
        std::fs::remove_dir_all(&parent).unwrap();
        std::fs::write(&parent, b"not a directory").unwrap();
        assert!(policy.set("alice", false).is_err());
        assert!(policy.enabled("alice", true));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
