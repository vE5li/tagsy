//! The `From` impls that map each internal daemon failure onto
//! [`ApiError`](tagsy_api::ApiError).
//!
//! The [`ApiError`] enum itself lives in `tagsy-api` (it crosses the port); the
//! conversions stay here because they reference daemon-internal error types
//! (`DatabaseError`, `FetchError`, ...) that `tagsy-api` deliberately cannot
//! see. The orphan rule permits this: `tagsyd` owns every *source* type, so it
//! may implement `From<Source> for` the foreign `ApiError`.

use tagsy_api::ApiError;

use crate::catalog::messages::{FetchError, PreviewError, RestoreError};
use crate::store::DatabaseError;

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::MissingFile | DatabaseError::MissingTag => ApiError::UnknownId,
            DatabaseError::AmbiguousIdPrefix(prefix) => ApiError::AmbiguousId(prefix),
            DatabaseError::InvalidTagName => {
                ApiError::InvalidArgument("invalid tag name".to_owned())
            }
            DatabaseError::InvalidColor => ApiError::InvalidArgument("invalid color".to_owned()),
            DatabaseError::CantTagItself => {
                ApiError::InvalidArgument("a tag cannot be its own subtag".to_owned())
            }
            // Everything left — a raw SQL failure, an unopenable database, a
            // non-UTF-8 path — is a storage-layer fault the UI can do nothing
            // about. Rendering it into `Internal` keeps `ApiError` free of
            // foreign payloads (see the type docs) at no cost to the UI, which
            // only ever displayed the text anyway.
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl From<FetchError> for ApiError {
    fn from(error: FetchError) -> Self {
        match error {
            // The file is in the catalog; no reachable peer has its bytes.
            FetchError::NotAvailable => ApiError::ContentUnavailable,
            FetchError::TimedOut | FetchError::ShuttingDown => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<RestoreError> for ApiError {
    fn from(error: RestoreError) -> Self {
        match error {
            // No source still held the bytes to restore from.
            RestoreError::NotAvailable => ApiError::ContentUnavailable,
            RestoreError::NotDeleted => ApiError::InvalidArgument(error.to_string()),
            RestoreError::ShuttingDown => ApiError::Internal(error.to_string()),
        }
    }
}

impl From<PreviewError> for ApiError {
    fn from(error: PreviewError) -> Self {
        match error {
            // The file id isn't in the catalog at all.
            PreviewError::UnknownFile => ApiError::UnknownId,
            // Transient: local generation produced nothing and no reachable
            // peer served one. Mirrors `FetchError::NotAvailable` — the entity
            // exists, its preview just couldn't be obtained right now, so the
            // UI should offer a retry rather than treat it as permanent.
            PreviewError::Unavailable => ApiError::ContentUnavailable,
            PreviewError::ShuttingDown => ApiError::Internal(error.to_string()),
        }
    }
}

// `From<FileBytesError>` is *not* here: `FileBytesError` moved to `tagsy-core`
// and `ApiError` lives in `tagsy-api`, so with both foreign to `tagsyd` the
// orphan rule forbids the impl here. It lives in `tagsy-api` instead (where
// `ApiError` is local).
