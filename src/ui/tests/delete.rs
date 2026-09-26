use crate::engine::{DownloadAction, DownloadStatus};
use crate::ui::delete::{DeleteTarget, history_delete_failure};
use std::io::{Error, ErrorKind};

#[test]
fn delete_target_preserves_exact_confirmation_token() {
    let completed = DeleteTarget {
        session_id: 41,
        status: DownloadStatus::Completed,
        completed_only: true,
    };
    match completed.action(true) {
        DownloadAction::Remove {
            expected_session_id,
            expected_status,
            delete_file,
            completed_only,
        } => {
            assert_eq!(expected_session_id, 41);
            assert_eq!(expected_status, DownloadStatus::Completed);
            assert!(delete_file);
            assert!(completed_only);
        }
        _ => panic!("delete target must produce Remove"),
    }
}

#[test]
fn a_missing_file_counts_as_deleted() {
    assert!(history_delete_failure(Ok(())).is_none());
    assert!(
        history_delete_failure(Err(Error::from(ErrorKind::NotFound))).is_none(),
        "a file that is already gone is what the user asked for"
    );
    assert_eq!(
        history_delete_failure(Err(Error::from(ErrorKind::PermissionDenied)))
            .map(|error| error.kind()),
        Some(ErrorKind::PermissionDenied),
        "every other failure is reported"
    );
}
