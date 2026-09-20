use super::MainWindow;
use super::app::{DeleteTarget, send_action};
use super::platform::default_download_directory;
use super::projection::{should_project_snapshot, update_window_state};
use slint::ComponentHandle;

#[test]
fn snapshot_projection_handles_initial_changed_and_closed_updates() {
    let (tx, mut rx) = tokio::sync::watch::channel(0_u8);
    let mut initial = true;

    {
        let snapshot = rx.borrow_and_update();
        assert!(should_project_snapshot(&snapshot, &mut initial));
        assert_eq!(*snapshot, 0);
    }
    {
        let snapshot = rx.borrow_and_update();
        assert!(!should_project_snapshot(&snapshot, &mut initial));
    }

    assert!(tx.send(1).is_ok());
    {
        let snapshot = rx.borrow_and_update();
        assert!(should_project_snapshot(&snapshot, &mut initial));
        assert_eq!(*snapshot, 1);
    }
    {
        let snapshot = rx.borrow_and_update();
        assert!(!should_project_snapshot(&snapshot, &mut initial));
    }

    assert!(tx.send(2).is_ok());
    drop(tx);
    assert!(rx.has_changed().is_err());
    {
        let snapshot = rx.borrow_and_update();
        assert!(should_project_snapshot(&snapshot, &mut initial));
        assert_eq!(*snapshot, 2);
    }
    {
        let snapshot = rx.borrow_and_update();
        assert!(!should_project_snapshot(&snapshot, &mut initial));
    }
}

#[test]
fn delete_target_preserves_exact_confirmation_token() {
    use crate::engine::{DownloadAction, DownloadStatus};

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
fn controls_and_filters_support_pointer_and_keyboard() -> Result<(), Box<dyn std::error::Error>> {
    use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
    use slint::platform::{Clipboard, Platform, PointerEventButton, WindowAdapter, WindowEvent};
    use std::cell::RefCell;
    use std::rc::Rc;

    struct TestPlatform(Rc<MinimalSoftwareWindow>, Rc<RefCell<String>>);
    impl Platform for TestPlatform {
        fn create_window_adapter(&self) -> Result<Rc<dyn WindowAdapter>, slint::PlatformError> {
            Ok(self.0.clone())
        }

        fn set_clipboard_text(&self, text: &str, clipboard: Clipboard) {
            if clipboard == Clipboard::DefaultClipboard {
                *self.1.borrow_mut() = text.into();
            }
        }

        fn clipboard_text(&self, clipboard: Clipboard) -> Option<String> {
            (clipboard == Clipboard::DefaultClipboard).then(|| self.1.borrow().clone())
        }
    }

    let window = MinimalSoftwareWindow::new(RepaintBufferType::ReusedBuffer);
    let clipboard = Rc::new(RefCell::new(String::new()));
    slint::platform::set_platform(Box::new(TestPlatform(window.clone(), clipboard.clone())))?;
    let ui = MainWindow::new()?;
    ui.show()?;
    let click = |x, y| {
        let position = slint::LogicalPosition::new(x, y);
        window.dispatch_event(WindowEvent::PointerPressed {
            position,
            button: PointerEventButton::Left,
        });
        window.dispatch_event(WindowEvent::PointerReleased {
            position,
            button: PointerEventButton::Left,
        });
    };

    for (width, height) in [(960, 540), (760, 420)] {
        window.set_size(slint::PhysicalSize::new(width, height));
        let mut pixels = vec![slint::Rgb8Pixel::default(); (width * height) as usize];
        let mut render = || {
            window.draw_if_needed(|renderer| {
                renderer.render(&mut pixels, width as usize);
            });
        };
        ui.set_all_downloads_expanded(true);
        render();
        // Every label and trailing blank area selects the same full-width row.
        for x in [90.0, 185.0] {
            ui.set_selected_category(6);
            click(x, 124.0);
            assert_eq!(ui.get_selected_category(), 0);
            render();
        }
        for category in 1..10 {
            let gap = if category >= 8 {
                12
            } else if category >= 6 {
                6
            } else {
                0
            };
            let y = (124 + category * 26 + gap) as f32;
            for x in [90.0, 185.0] {
                click(x, y);
                assert_eq!(
                    ui.get_selected_category(),
                    category.min(7),
                    "Unimplemented categories must not change the selection"
                );
                render();
            }
        }
        click(90.0, 150.0);
        assert_eq!(ui.get_selected_category(), 1);
        click(90.0, 124.0);
        assert_eq!(ui.get_selected_category(), 0);
        assert!(ui.get_all_downloads_expanded());
        click(90.0, 124.0);
        assert!(!ui.get_all_downloads_expanded());
        render();
        click(90.0, 124.0);
        assert!(ui.get_all_downloads_expanded());
        click(18.0, 124.0);
        assert!(!ui.get_all_downloads_expanded());
        assert_eq!(ui.get_selected_category(), 0);
        render();
        click(90.0, 156.0);
        assert_eq!(ui.get_selected_category(), 6);
        click(90.0, 124.0);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::RightArrow.into(),
        });
        assert!(ui.get_all_downloads_expanded());
        render();
        click(90.0, 150.0);
        ui.set_selected_category(0);
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(ui.get_selected_category(), 1);
        render();

        let left = width as f32 / 2.0 - 230.0;
        let top = height as f32 / 2.0 - 124.0;
        ui.set_show_add_dialog(true);
        ui.set_url_text("".into());
        ui.set_dest_dir_text(
            default_download_directory()
                .to_string_lossy()
                .into_owned()
                .into(),
        );
        ui.set_streams_count(8.0);
        render();
        assert_eq!(ui.get_streams_count(), 8.0);
        click(left + 395.0, top + 210.0);
        assert!(
            ui.get_show_add_dialog(),
            "Empty URL keeps Download disabled"
        );

        click(left + 84.0, top + 166.0);
        assert_eq!(ui.get_streams_count(), 1.0);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::RightArrow.into(),
        });
        assert_eq!(ui.get_streams_count(), 2.0);
        click(left + 366.0, top + 166.0);
        assert_eq!(ui.get_streams_count(), 16.0);

        click(left + 120.0, top + 74.0);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: "https://example.com/file.zip".into(),
        });
        assert_eq!(ui.get_url_text(), "https://example.com/file.zip");
        let shortcut = |text: &str| {
            let modifier = if cfg!(target_os = "macos") {
                slint::platform::Key::Meta
            } else {
                slint::platform::Key::Control
            };
            window.dispatch_event(WindowEvent::KeyPressed {
                text: modifier.into(),
            });
            window.dispatch_event(WindowEvent::KeyPressed { text: text.into() });
            window.dispatch_event(WindowEvent::KeyReleased { text: text.into() });
            window.dispatch_event(WindowEvent::KeyReleased {
                text: modifier.into(),
            });
        };
        shortcut("a");
        shortcut("x");
        assert!(ui.get_url_text().is_empty());
        assert_eq!(*clipboard.borrow(), "https://example.com/file.zip");
        shortcut("v");
        assert_eq!(ui.get_url_text(), "https://example.com/file.zip");
        let context_position = slint::LogicalPosition::new(left + 120.0, top + 74.0);
        let open_menu = || {
            window.dispatch_event(WindowEvent::PointerPressed {
                position: context_position,
                button: PointerEventButton::Right,
            });
            window.dispatch_event(WindowEvent::PointerReleased {
                position: context_position,
                button: PointerEventButton::Right,
            });
        };
        open_menu();
        render();
        for _ in 0..3 {
            window.dispatch_event(WindowEvent::KeyPressed {
                text: slint::platform::Key::DownArrow.into(),
            });
        }
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert_eq!(
            ui.get_url_text(),
            "https://example.com/file.ziphttps://example.com/file.zip",
            "The right-click menu supports keyboard Paste"
        );
        shortcut("a");
        *clipboard.borrow_mut() = "https://example.com/replacement.zip".into();
        open_menu();
        render();
        let paste_position =
            slint::LogicalPosition::new(context_position.x + 100.0, context_position.y + 74.0);
        window.dispatch_event(WindowEvent::PointerMoved {
            position: paste_position,
        });
        window.dispatch_event(WindowEvent::PointerMoved {
            position: slint::LogicalPosition::new(context_position.x + 210.0, paste_position.y),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert_eq!(
            ui.get_url_text(),
            "https://example.com/file.ziphttps://example.com/file.zip",
            "Paste hover clears when the pointer leaves the row"
        );
        click(context_position.x + 20.0, context_position.y + 74.0);
        assert_eq!(
            ui.get_url_text(),
            "https://example.com/replacement.zip",
            "Right-click preserves selection and pointer Paste replaces it"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Menu.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed { text: "#".into() });
        assert_eq!(
            ui.get_url_text(),
            "https://example.com/replacement.zip#",
            "Escape returns keyboard focus to the input"
        );
        let browsed = Rc::new(std::cell::Cell::new(0));
        let count = browsed.clone();
        ui.on_browse_folder(move || count.set(count.get() + 1));
        click(left + 400.0, top + 122.0);
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(browsed.get(), 2, "Browse supports pointer and keyboard");
        render();
        click(left + 310.0, top + 210.0);
        assert!(!ui.get_show_add_dialog());
        ui.set_show_add_dialog(true);
        let started = Rc::new(std::cell::Cell::new(false));
        let flag = started.clone();
        ui.on_start_download(move || flag.set(true));
        render();
        click(left + 395.0, top + 210.0);
        assert!(started.get());
        assert!(!ui.get_show_add_dialog());
        render();

        let stopped = Rc::new(std::cell::Cell::new(0));
        let count = stopped.clone();
        ui.on_pause_download(move || count.set(count.get() + 1));
        let resumed = Rc::new(std::cell::Cell::new(0));
        let count = resumed.clone();
        ui.on_resume_download(move || count.set(count.get() + 1));
        let selected_delete_requests = Rc::new(std::cell::Cell::new(0));
        let count = selected_delete_requests.clone();
        let window_weak = ui.as_weak();
        ui.on_request_delete_selected(move || {
            count.set(count.get() + 1);
            if let Some(window) = window_weak.upgrade() {
                window.set_delete_filename(window.get_active_filename());
                window.set_delete_completed_only(false);
                window.set_delete_target_completed(window.get_is_completed());
                window.set_delete_file(false);
                window.set_show_delete_dialog(true);
            }
        });
        let completed_delete_requests = Rc::new(std::cell::Cell::new(0));
        let count = completed_delete_requests.clone();
        let window_weak = ui.as_weak();
        ui.on_request_delete_completed(move || {
            count.set(count.get() + 1);
            if let Some(window) = window_weak.upgrade() {
                window.set_delete_filename(window.get_active_filename());
                window.set_delete_completed_only(true);
                window.set_delete_target_completed(true);
                window.set_delete_file(false);
                window.set_show_delete_dialog(true);
            }
        });
        let confirmed_deletes = Rc::new(RefCell::new(Vec::new()));
        let payloads = confirmed_deletes.clone();
        let window_weak = ui.as_weak();
        ui.on_confirm_delete(move |delete_file| {
            payloads.borrow_mut().push(delete_file);
            if let Some(window) = window_weak.upgrade() {
                window.invoke_close_delete_dialog();
            }
        });

        use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
        let mut snapshot = DownloadSnapshot {
            session_id: 7,
            filename: "archive.zip".into(),
            status: DownloadStatus::Downloading,
            resumable: true,
            ..Default::default()
        };
        update_window_state(&ui, &snapshot);
        assert_eq!(ui.get_total_items(), 1);
        ui.set_selected_category(6);
        ui.set_selected_row(1);
        render();
        assert!(ui.get_active_row_visible());
        assert!(
            !ui.get_can_stop(),
            "Stop only targets the selected download"
        );
        assert!(!ui.get_can_delete_selected());
        assert!(ui.get_can_stop_all());
        click(166.0, 50.0);
        assert_eq!(stopped.get(), 0);
        click(230.0, 50.0);
        assert_eq!(stopped.get(), 1, "Stop All ignores selection");
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(stopped.get(), 2, "Toolbar supports keyboard activation");
        click(300.0, 116.0);
        assert_eq!(ui.get_selected_row(), 0);
        ui.set_selected_row(1);
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(
            ui.get_selected_row(),
            0,
            "Download selection supports keyboard"
        );
        assert!(ui.get_can_stop());
        click(166.0, 50.0);
        assert_eq!(stopped.get(), 3);

        assert!(ui.get_can_delete_selected());
        assert!(!ui.get_can_delete_completed());
        click(298.0, 50.0);
        assert_eq!(selected_delete_requests.get(), 1);
        assert!(ui.get_show_delete_dialog());
        assert_eq!(ui.get_delete_filename(), "archive.zip");
        assert!(!ui.get_delete_completed_only());
        assert!(!ui.get_delete_file(), "file deletion defaults to unchecked");
        render();

        click(230.0, 50.0);
        assert_eq!(stopped.get(), 3, "modal overlay blocks the toolbar");
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert!(!ui.get_show_delete_dialog(), "Cancel has initial focus");
        assert!(confirmed_deletes.borrow().is_empty());

        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert!(ui.get_show_delete_dialog());
        render();
        let delete_top = height as f32 / 2.0 - 90.0;
        click(width as f32 / 2.0 - 150.0, delete_top + 146.0);
        assert!(
            ui.get_delete_file(),
            "pointer toggles and focuses the checkbox"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert!(!ui.get_show_delete_dialog());
        assert!(
            confirmed_deletes.borrow().is_empty(),
            "Tab follows pointer focus to Cancel instead of Delete"
        );
        assert!(!ui.get_delete_file(), "Cancel resets the checkbox");

        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert!(ui.get_show_delete_dialog(), "focus returns to Delete");
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert_eq!(&*confirmed_deletes.borrow(), &[false]);

        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Shift.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyReleased {
            text: slint::platform::Key::Shift.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert!(ui.get_delete_file(), "Space toggles the focused checkbox");
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert_eq!(&*confirmed_deletes.borrow(), &[false, true]);

        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert!(!ui.get_delete_file(), "checkbox resets for every request");
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        assert!(!ui.get_show_delete_dialog());
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert!(
            ui.get_show_delete_dialog(),
            "Escape restores focus to the requesting toolbar action"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });

        snapshot.status = DownloadStatus::Paused;
        update_window_state(&ui, &snapshot);
        render();
        assert!(ui.get_can_resume());
        assert!(!ui.get_can_stop_all());
        assert_eq!(ui.get_active_status(), "Stopped");
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(stopped.get(), 3, "A disabled Stop ignores keyboard input");
        ui.set_selected_row(1);
        click(102.0, 50.0);
        assert_eq!(resumed.get(), 0, "Resume cannot target another row");
        ui.set_selected_row(0);
        click(102.0, 50.0);
        assert_eq!(resumed.get(), 1);
        snapshot.resumable = false;
        update_window_state(&ui, &snapshot);
        render();
        click(102.0, 50.0);
        assert_eq!(resumed.get(), 2, "Non-range downloads can restart");
        snapshot.status = DownloadStatus::Failed("Offline".into());
        snapshot.resumable = true;
        update_window_state(&ui, &snapshot);
        assert!(
            ui.get_can_resume(),
            "Network failures retain a resume action"
        );
        assert_eq!(ui.get_active_status(), "Failed");
        assert_eq!(ui.get_active_error_message(), "Offline");
        render();
        click(width as f32 / 2.0 + 175.0, height as f32 / 2.0 + 68.0);
        assert!(
            ui.get_active_error_message().is_empty(),
            "The error dialog can be dismissed"
        );
        snapshot.resumable = false;
        update_window_state(&ui, &snapshot);
        assert!(!ui.get_can_resume(), "Unsafe failures cannot resume");
        ui.set_selected_category(7);
        assert!(!ui.get_active_row_visible());
        assert!(!ui.get_can_resume(), "Hidden selection cannot be resumed");
        for status in [
            DownloadStatus::Connecting,
            DownloadStatus::Downloading,
            DownloadStatus::Paused,
            DownloadStatus::Failed("Offline".into()),
            DownloadStatus::Completed,
            DownloadStatus::Idle,
        ] {
            snapshot.status = status.clone();
            update_window_state(&ui, &snapshot);
            ui.set_selected_category(6);
            assert_eq!(
                ui.get_active_row_visible(),
                !matches!(status, DownloadStatus::Completed | DownloadStatus::Idle)
            );
            assert_eq!(
                ui.get_can_stop_all(),
                matches!(
                    status,
                    DownloadStatus::Connecting | DownloadStatus::Downloading
                )
            );
            render();
        }
        snapshot.status = DownloadStatus::Completed;
        update_window_state(&ui, &snapshot);
        ui.set_selected_category(6);
        ui.set_selected_row(1);
        assert!(!ui.get_can_delete_selected());
        assert!(
            ui.get_can_delete_completed(),
            "Delete Completed ignores row and category selection"
        );
        render();
        click(380.0, 50.0);
        assert_eq!(completed_delete_requests.get(), 1);
        assert!(ui.get_show_delete_dialog());
        assert!(ui.get_delete_completed_only());
        assert!(!ui.get_delete_file());
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });

        for category in 0..10 {
            ui.set_selected_category(category);
            assert_eq!(ui.get_active_row_visible(), matches!(category, 0 | 1 | 7));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        assert!(send_action(&ui, &tx, DownloadAction::Pause));
        assert!(!send_action(&ui, &tx, DownloadAction::Resume));
        assert!(ui.get_action_error_message().contains("busy"));
        drop(rx);
        assert!(!send_action(&ui, &tx, DownloadAction::Pause));
        assert!(ui.get_action_error_message().contains("unavailable"));
        ui.set_action_error_message("".into());
        update_window_state(&ui, &DownloadSnapshot::default());
        render();

        use slint::Model;
        assert_eq!(ui.get_sample_downloads().row_count(), 0);
        assert_eq!(ui.get_total_items(), 0);
        render();
        ui.set_has_active_download(true);
        assert_eq!(ui.get_total_items(), 1);
        ui.set_has_active_download(false);
    }
    Ok(())
}
