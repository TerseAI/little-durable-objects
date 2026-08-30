//! Derived durable-object lifecycle. Cache residency is never authoritative.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableExistence {
    Present,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingState {
    Unowned,
    Serving,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalCacheState {
    Missing,
    Present,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableObjectStatus {
    Serving,
    Warm,
    Cold,
    Deleted,
}

impl DurableObjectStatus {
    pub fn derive(
        existence: DurableExistence,
        serving: ServingState,
        cache: LocalCacheState,
    ) -> Self {
        match (existence, serving, cache) {
            (DurableExistence::Deleted, _, _) => Self::Deleted,
            (DurableExistence::Present, ServingState::Serving, _) => Self::Serving,
            (DurableExistence::Present, ServingState::Unowned, LocalCacheState::Present) => {
                Self::Warm
            }
            (DurableExistence::Present, ServingState::Unowned, LocalCacheState::Missing) => {
                Self::Cold
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_wins_over_runtime_and_cache_state() {
        assert_eq!(
            DurableObjectStatus::derive(
                DurableExistence::Deleted,
                ServingState::Serving,
                LocalCacheState::Present,
            ),
            DurableObjectStatus::Deleted
        );
    }

    #[test]
    fn an_unowned_cached_object_is_warm() {
        assert_eq!(
            DurableObjectStatus::derive(
                DurableExistence::Present,
                ServingState::Unowned,
                LocalCacheState::Present,
            ),
            DurableObjectStatus::Warm
        );
    }
}
