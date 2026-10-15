//! Bounded, exact-key result retention for explicitly reusable computations.
use serde_json::Value;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

struct Entry {
    value: Value,
    expires: Instant,
    bytes: usize,
    used: u64,
}

pub struct Cache {
    entries: BTreeMap<String, Entry>,
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
    clock: u64,
}

impl Cache {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            max_entries,
            max_bytes,
            bytes: 0,
            clock: 0,
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= entry.bytes;
        }
    }

    pub fn get(&mut self, key: &str, now: Instant) -> Option<Value> {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| now >= entry.expires)
        {
            self.remove(key);
        }
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.used = self.clock;
        Some(entry.value.clone())
    }

    pub fn insert(&mut self, key: String, value: Value, ttl: Duration, now: Instant) -> bool {
        // Account for both key and encoded payload. This bounds retained serialized data,
        // not allocator overhead; the entry count provides a second independent bound.
        let bytes = key.len().saturating_add(value.to_string().len());
        if bytes > self.max_bytes || self.max_entries == 0 || ttl.is_zero() {
            return false;
        }
        let Some(expires) = now.checked_add(ttl) else {
            return false;
        };
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, v)| now >= v.expires)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            self.remove(&key);
        }
        self.remove(&key);
        while self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, value)| value.used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
        self.clock = self.clock.saturating_add(1);
        self.bytes += bytes;
        self.entries.insert(
            key,
            Entry {
                value,
                expires,
                bytes,
                used: self.clock,
            },
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expiry_and_lru_are_bounded_without_sleeping() {
        let now = Instant::now();
        let mut cache = Cache::new(2, 1024);
        assert!(cache.insert("a".into(), json!(1), Duration::from_secs(1), now));
        cache.insert("b".into(), json!(2), Duration::from_secs(2), now);
        assert_eq!(cache.get("a", now), Some(json!(1)));
        cache.insert("c".into(), json!(3), Duration::from_secs(2), now);
        assert_eq!(cache.get("b", now), None);
        assert_eq!(cache.get("a", now + Duration::from_secs(1)), None);
        assert_eq!(cache.get("c", now), Some(json!(3)));
        assert!(cache.entries.len() <= 2);
    }

    #[test]
    fn payload_limit_and_replacement_accounting() {
        let mut cache = Cache::new(10, 8);
        let now = Instant::now();
        assert!(!cache.insert("x".into(), json!("too big"), Duration::from_secs(1), now));
        assert!(cache.insert("x".into(), json!(1), Duration::from_secs(1), now));
        assert!(cache.insert("x".into(), json!(2), Duration::from_secs(1), now));
        assert_eq!(cache.bytes, 2);
        assert_eq!(cache.get("x", now), Some(json!(2)));
    }
}
