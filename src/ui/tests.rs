use super::MainWindow;
use super::app::{DeleteTarget, complete_history_delete, history_delete_failure, send_action};
use super::platform::default_download_directory;
use super::projection::{
    history_table_item, should_project_snapshot, sort_items, update_window_state,
};
use crate::history::{HistoryEntry, HistoryStore, downloaded_label};
use slint::ComponentHandle;
use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
use slint::platform::{Clipboard, Platform, WindowAdapter};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

fn finished_entry(id: i32, filename: &str, total_bytes: u64) -> HistoryEntry {
    HistoryEntry {
        id,
        url: format!("https://example.com/{filename}"),
        filename: filename.to_string(),
        save_path: PathBuf::from(filename),
        total_bytes,
        completed_unix_ms: 1_700_000_000_000 + id as u64,
    }
}

type TestContext = (Rc<MinimalSoftwareWindow>, Rc<RefCell<String>>);

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

thread_local! {
    static TEST_CONTEXT: RefCell<Option<TestContext>> = const { RefCell::new(None) };
}

// Slint platforms are thread-local and can only be set once per thread.
fn install_test_platform() -> Result<TestContext, Box<dyn std::error::Error>> {
    if let Some((window, clipboard)) = TEST_CONTEXT.with(|slot| slot.borrow().clone()) {
        return Ok((window, clipboard));
    }
    let window = MinimalSoftwareWindow::new(RepaintBufferType::ReusedBuffer);
    let clipboard = Rc::new(RefCell::new(String::new()));
    slint::platform::set_platform(Box::new(TestPlatform(window.clone(), clipboard.clone())))?;
    TEST_CONTEXT.with(|slot| *slot.borrow_mut() = Some((window.clone(), clipboard.clone())));
    Ok((window, clipboard))
}

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
    use slint::platform::{PointerEventButton, WindowEvent};

    let (window, clipboard) = install_test_platform()?;
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

    // Toolbar "Options" button: last button of the action row, right of "Delete Completed".
    let click_options = || click(470.0, 52.0);

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

        assert!(!ui.get_sidebar_collapsed());
        click(186.0, 92.0);
        assert!(ui.get_sidebar_collapsed());
        render();
        click(14.0, 92.0);
        assert!(!ui.get_sidebar_collapsed());
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
            window.dispatch_event(WindowEvent::KeyPressed {
                text: slint::platform::Key::Control.into(),
            });
            window.dispatch_event(WindowEvent::KeyPressed { text: text.into() });
            window.dispatch_event(WindowEvent::KeyReleased { text: text.into() });
            window.dispatch_event(WindowEvent::KeyReleased {
                text: slint::platform::Key::Control.into(),
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
            1,
            "Space acts on the row the list has selected"
        );
        ui.set_selected_row(0);
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(
            ui.get_selected_row(),
            0,
            "Space keeps the active download selected"
        );
        assert!(ui.get_can_stop());
        click(166.0, 50.0);
        assert_eq!(stopped.get(), 3);

        snapshot.status = DownloadStatus::Paused;
        update_window_state(&ui, &snapshot);
        render();
        assert!(ui.get_can_resume());
        assert!(!ui.get_can_stop_all());
        assert_eq!(ui.get_active_status(), "Stopped");
        window.dispatch_event(WindowEvent::KeyPressed { text: " ".into() });
        assert_eq!(stopped.get(), 3, "A disabled Stop ignores keyboard input");

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
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
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
        ui.set_completed_listed(true);
        ui.set_selected_category(6);
        ui.set_selected_row(1);
        assert!(!ui.get_can_delete_selected());
        assert!(
            ui.get_can_delete_completed(),
            "Delete Completed ignores row and category selection"
        );
        // Once the listed row of the finished download is deleted, it is no longer offered.
        ui.set_completed_listed(false);
        assert!(!ui.get_can_delete_completed());
        ui.set_completed_listed(true);
        render();
        click(380.0, 50.0);
        assert_eq!(completed_delete_requests.get(), 1);
        assert!(ui.get_show_delete_dialog());
        assert!(ui.get_delete_completed_only());
        assert!(!ui.get_delete_file());
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });

        // The dialog mirrors the projected tab, so pin it to keep both window sizes deterministic.
        ui.set_options_selected_tab(0);
        click_options();
        assert!(
            ui.get_show_options_dialog(),
            "Options button opens options dialog"
        );
        assert_eq!(
            ui.get_options_selected_tab(),
            0,
            "Options opens on the first tab"
        );
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::RightArrow.into(),
        });
        assert_eq!(
            ui.get_options_selected_tab(),
            1,
            "Right arrow advances the options tab"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::LeftArrow.into(),
        });
        assert_eq!(ui.get_options_selected_tab(), 0);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::LeftArrow.into(),
        });
        assert_eq!(
            ui.get_options_selected_tab(),
            0,
            "The first options tab cannot move left"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        assert!(
            !ui.get_show_options_dialog(),
            "Escape dismisses options dialog"
        );

        // The options dialog is 560x380 and centered; its tab strip starts 52px below its top edge.
        let options_left = width as f32 / 2.0 - 280.0;
        let options_top = height as f32 / 2.0 - 190.0;

        click_options();
        assert!(
            ui.get_show_options_dialog(),
            "Options reopens after dismissal"
        );
        assert_eq!(
            ui.get_options_selected_tab(),
            0,
            "The reopened dialog keeps the last selected tab"
        );
        render();
        // The strip is 528px wide inside the dialog's 16px padding and splits into seven 75.7px
        // tabs, so tab 1 spans x = 92..165 of the dialog; the strip's vertical center is 68px in.
        let tab_1 = (options_left + 128.0, options_top + 68.0);
        click(tab_1.0, tab_1.1);
        assert_eq!(
            ui.get_options_selected_tab(),
            1,
            "Clicking a tab selects it on the first click"
        );
        let capture = || {
            window.request_redraw();
            let mut probe = vec![slint::Rgb8Pixel::default(); (width * height) as usize];
            window.draw_if_needed(|renderer| {
                renderer.render(&mut probe, width as usize);
            });
            probe
        };
        let pixel = |probe: &[slint::Rgb8Pixel], x: f32, y: f32| {
            probe[(y as usize) * (width as usize) + (x as usize)]
        };
        let probe = capture();
        assert_ne!(
            pixel(&probe, tab_1.0, options_top + 52.0),
            slint::Rgb8Pixel {
                r: 0x8b,
                g: 0xa9,
                b: 0xd6
            },
            "Mouse click on tab does not show focus ring"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert!(
            !ui.get_show_options_dialog(),
            "Tab keeps focus inside the dialog and Return activates OK"
        );

        click_options();
        render();
        ui.set_options_selected_tab(6);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::RightArrow.into(),
        });
        assert_eq!(
            ui.get_options_selected_tab(),
            6,
            "The last options tab cannot move right"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        assert!(
            !ui.get_show_options_dialog(),
            "Escape still dismisses the options dialog"
        );

        click_options();
        render();
        ui.set_options_selected_tab(1);
        let ring = slint::Rgb8Pixel {
            r: 0x8b,
            g: 0xa9,
            b: 0xd6,
        };
        let probe_initial = capture();
        assert_ne!(
            pixel(&probe_initial, options_left + 428.0, options_top + 326.0),
            ring,
            "Opening options dialog does not show focus ring on OK button"
        );

        // Tab from initial state moves focus to the tab strip.
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        assert_eq!(ui.get_options_selected_tab(), 1);
        let probe = capture();
        assert!(
            (164..=168)
                .any(|dx| pixel(&probe, options_left + dx as f32, options_top + 68.0) == ring),
            "Keyboard-focused tab draws focus ring on its right border without divider overlap"
        );

        // The close button was removed, so clicking the header corner does not dismiss options dialog.
        click(options_left + 532.0, options_top + 28.0);
        assert!(
            ui.get_show_options_dialog(),
            "Options dialog remains open after clicking header corner (close button removed)"
        );

        // Cancel button is in the 36px footer (16px from bottom, 16px from right, 72px wide).
        click(options_left + 508.0, options_top + 346.0);
        assert!(
            !ui.get_show_options_dialog(),
            "Clicking Cancel dismisses the options dialog"
        );
        let probe = capture();
        assert_ne!(
            pixel(&probe, 470.0, 27.0),
            ring,
            "Closing options dialog with mouse does not show focus ring on toolbar button"
        );

        // Test Add URL button and AddDownloadDialog focus rotation.
        click(39.0, 52.0);
        assert!(ui.get_show_add_dialog(), "Add URL button opens dialog");
        render();
        // Tab rotates within AddDownloadDialog across its focus stops.
        // With empty URL, Download is disabled, so Cancel (4) wraps directly to URL (0).
        for _ in 0..4 {
            window.dispatch_event(WindowEvent::KeyPressed {
                text: slint::platform::Key::Tab.into(),
            });
        }
        // At Cancel button: Tab should wrap to URL (0) and stay inside dialog.
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        assert!(
            ui.get_show_add_dialog(),
            "Tab from Cancel keeps Add Download dialog open and focused"
        );
        // Shift+Tab from URL (0) should wrap backward to Cancel (4), skipping disabled Download.
        // Return then activates Cancel, proving the backward wrap reached the Cancel button
        // (with the URL empty, Return on any other stop leaves the dialog open).
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Shift.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Tab.into(),
        });
        window.dispatch_event(WindowEvent::KeyReleased {
            text: slint::platform::Key::Shift.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert!(
            !ui.get_show_add_dialog(),
            "Shift+Tab from URL wraps to Cancel, whose Return cancels the dialog"
        );
        // Pressing Escape dismisses Add Download dialog.
        click(39.0, 52.0);
        assert!(ui.get_show_add_dialog(), "Add URL reopens the dialog");
        render();
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        assert!(
            !ui.get_show_add_dialog(),
            "Escape dismisses Add Download dialog"
        );

        // The Download button is accent-filled, so its focus ring must be drawn outside
        // the fill: ring pixel, 1px panel gap, then the fill. A ring on the fill itself
        // would be the same blue and therefore invisible.
        click(39.0, 52.0);
        render();
        for _ in 0..5 {
            window.dispatch_event(WindowEvent::KeyPressed {
                text: slint::platform::Key::Tab.into(),
            });
        }
        window.request_redraw();
        let mut probe = vec![slint::Rgb8Pixel::default(); (width * height) as usize];
        window.draw_if_needed(|renderer| {
            renderer.render(&mut probe, width as usize);
        });
        let pixel = |x: f32, y: f32| probe[(y as usize) * (width as usize) + (x as usize)];
        assert_eq!(
            pixel(left + 350.0, top + 210.0),
            slint::Rgb8Pixel {
                r: 0x8b,
                g: 0xa9,
                b: 0xd6
            },
            "Focused Download button draws the focus ring outside its accent fill"
        );
        assert_eq!(
            pixel(left + 351.0, top + 210.0),
            slint::Rgb8Pixel {
                r: 0x1f,
                g: 0x1f,
                b: 0x23
            },
            "A 1px panel gap separates the ring from the accent fill"
        );
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Escape.into(),
        });
        assert!(
            !ui.get_show_add_dialog(),
            "Escape dismisses the dialog again"
        );

        // Picking a category reports the change, so the selected row is re-checked on the
        // event instead of polling the category on a timer.
        let category_changes = Rc::new(std::cell::Cell::new(0));
        ui.on_selected_category_changed({
            let category_changes = category_changes.clone();
            move || category_changes.set(category_changes.get() + 1)
        });
        click(100.0, 254.0);
        assert_eq!(
            ui.get_selected_category(),
            5,
            "\"Video\" is the fifth category row"
        );
        click(100.0, 176.0);
        assert_eq!(
            ui.get_selected_category(),
            2,
            "\"Documents\" is the second category row"
        );
        assert_eq!(
            category_changes.get(),
            2,
            "each sidebar pick is reported once"
        );

        // A finished download is never pinned; only an unfinished one follows the category filter.
        snapshot.status = DownloadStatus::Downloading;
        update_window_state(&ui, &snapshot);
        for category in 0..10 {
            ui.set_selected_category(category);
            assert_eq!(ui.get_active_row_visible(), matches!(category, 0 | 1 | 6));
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

    // Listed downloads are shown next to the active row, not instead of it.
    use slint::Model;
    let history = std::rc::Rc::new(slint::VecModel::<super::TableItem>::default());
    ui.set_sample_downloads(history.clone().into());
    history.push(history_table_item(&finished_entry(1, "file.bin", 1024)));
    assert_eq!(ui.get_sample_downloads().row_count(), 1);
    assert_eq!(ui.get_total_items(), 1);
    ui.set_has_active_download(true);
    assert_eq!(ui.get_total_items(), 2);
    ui.set_has_active_download(false);

    // History selection and deletion updates store and window
    {
        use super::app::{HistoryTracker, step_selection, update_selection_state};
        use crate::history::tests::TempFile;
        use std::cell::RefCell;

        let file = TempFile::new("delete-history-test");
        let mut store = HistoryStore::load_from(file.path().to_path_buf());
        let tracker = RefCell::new(HistoryTracker::new(1));

        let entry = crate::history::HistoryEntry {
            id: 1,
            url: "https://example.com/video.mp4".into(),
            filename: "video.mp4".into(),
            save_path: file.path().with_file_name("video.mp4"),
            total_bytes: 10_000,
            completed_unix_ms: 1_700_000_000_000,
        };
        store.record(entry.clone())?;

        // Create the dummy target file on disk
        std::fs::write(&entry.save_path, b"dummy data")?;
        assert!(entry.save_path.exists());

        // Initially nothing selected
        ui.set_selected_row(-1);
        update_selection_state(&ui, &store, -1);
        assert!(!ui.get_history_row_selected());
        assert!(!ui.get_can_delete_selected());

        // Select row 1 (video.mp4) in All category (0)
        ui.set_selected_category(0);
        ui.set_selected_row(1);
        update_selection_state(&ui, &store, 1);
        assert!(ui.get_history_row_selected());
        assert!(
            ui.get_can_delete_selected(),
            "delete is enabled for selected history entry"
        );

        // The list is one keyboard stop: a click puts the keyboard on it, and the arrows ask the
        // app to move the selection.
        let second = finished_entry(2, "second.bin", 2_048);
        store.record(second.clone())?;
        history.push(history_table_item(&second));
        let steps = Rc::new(std::cell::RefCell::new(Vec::new()));
        ui.on_step_selection({
            let steps = steps.clone();
            move |step| steps.borrow_mut().push(step)
        });
        let selected_requests = Rc::new(std::cell::RefCell::new(Vec::new()));
        ui.on_row_selected({
            let selected_requests = selected_requests.clone();
            move |id| selected_requests.borrow_mut().push(id)
        });
        click(300.0, 117.0);
        assert_eq!(
            ui.get_selected_row(),
            1,
            "the clicked listed row is selected"
        );
        assert_eq!(selected_requests.borrow().as_slice(), &[1]);

        // The arrows move the selection and report their direction.
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::DownArrow.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::UpArrow.into(),
        });
        assert_eq!(
            steps.borrow().as_slice(),
            &[1, -1],
            "the arrows reach the list with their direction"
        );
        ui.set_selected_row(-1);
        window.dispatch_event(WindowEvent::KeyPressed {
            text: slint::platform::Key::Return.into(),
        });
        assert_eq!(
            steps.borrow().as_slice(),
            &[1, -1, 1],
            "Return enters the list when nothing is selected"
        );

        // The app moves the selection through the listed rows, and stops at both ends.
        step_selection(&ui, &store, 1);
        assert_eq!(
            ui.get_selected_row(),
            1,
            "the first step picks the top listed row"
        );
        step_selection(&ui, &store, 1);
        assert_eq!(
            ui.get_selected_row(),
            2,
            "the down arrow moves to the next listed row"
        );
        assert!(
            ui.get_history_row_selected(),
            "a row reached this way can be deleted"
        );
        step_selection(&ui, &store, 1);
        assert_eq!(
            ui.get_selected_row(),
            2,
            "the selection stops at the last listed row"
        );
        step_selection(&ui, &store, -1);
        assert_eq!(
            ui.get_selected_row(),
            1,
            "the up arrow moves back to the previous row"
        );
        ui.set_selected_category(5);
        step_selection(&ui, &store, 1);
        assert_eq!(
            ui.get_selected_row(),
            1,
            "the arrows skip the rows the category hides"
        );
        ui.set_selected_category(0);

        // Change category to Compressed (1) -> video.mp4 does not match
        ui.set_selected_category(1);
        update_selection_state(&ui, &store, 1);
        assert!(!ui.get_history_row_selected());
        assert!(
            !ui.get_can_delete_selected(),
            "delete is disabled when category does not match"
        );

        // Change category to Video (5) -> video.mp4 matches
        ui.set_selected_category(5);
        update_selection_state(&ui, &store, 1);
        assert!(ui.get_history_row_selected());
        assert!(ui.get_can_delete_selected());

        // Deleting a listed download drops the file first, then the log entry and the row.
        let rows_before = ui.get_sample_downloads().row_count();
        std::fs::remove_file(&entry.save_path)?;
        // The toolbar holds the finished download, so its row is the one it offers to delete.
        tracker
            .borrow_mut()
            .observe(&crate::engine::DownloadSnapshot {
                session_id: 1,
                filename: "video.mp4".into(),
                status: crate::engine::DownloadStatus::Completed,
                total_bytes: Some(10_000),
                downloaded_bytes: 10_000,
                ..Default::default()
            })
            .expect("the finish is held for Delete Completed");
        ui.set_completed_listed(true);
        complete_history_delete(&ui, &tracker, &mut store, &history, entry.id, None);
        assert!(!entry.save_path.exists(), "file is removed from disk");
        assert!(
            store.entries().iter().all(|kept| kept.id != entry.id),
            "the deleted entry is removed from the store"
        );
        assert_eq!(
            ui.get_sample_downloads().row_count(),
            rows_before - 1,
            "the listed row is removed"
        );
        assert_eq!(ui.get_selected_row(), -1, "the removed row is not selected");
        assert!(!ui.get_history_row_selected());
        assert_eq!(ui.get_action_error_message(), "");
        assert!(
            !ui.get_completed_listed(),
            "Delete Completed is withdrawn once the finished download is no longer listed"
        );

        // Reloading store confirms disk file was rewritten
        let reloaded = HistoryStore::load_from(file.path().to_path_buf());
        assert!(reloaded.entries().iter().all(|kept| kept.id != entry.id));

        // A file deletion that failed keeps the row and reports why.
        store.record(entry.clone())?;
        history.push(history_table_item(&entry));
        ui.set_selected_row(entry.id);
        ui.set_history_row_selected(true);
        ui.set_show_delete_dialog(true);
        complete_history_delete(
            &ui,
            &tracker,
            &mut store,
            &history,
            entry.id,
            Some("Could not delete video.mp4: access is denied".to_string()),
        );
        assert!(
            store.entries().iter().any(|kept| kept.id == entry.id),
            "the entry survives for a retry"
        );
        assert_eq!(
            ui.get_sample_downloads().row_count(),
            rows_before,
            "the listed row survives for a retry"
        );
        assert_eq!(ui.get_selected_row(), entry.id);
        assert!(ui.get_history_row_selected());
        assert_eq!(
            ui.get_action_error_message(),
            "Could not delete video.mp4: access is denied",
            "the failure is reported instead of being swallowed"
        );
        assert!(!ui.get_show_delete_dialog());
    }

    // A header click reorders the listed rows and mirrors the sort state back to the window.
    {
        use super::TableItem;
        use super::app::apply_sort_request;
        use slint::Model;

        let sort_history = Rc::new(slint::VecModel::<TableItem>::default());
        sort_history.push(history_table_item(&finished_entry(1, "alpha.bin", 4096)));
        sort_history.push(history_table_item(&finished_entry(2, "beta.bin", 1024)));
        sort_history.push(history_table_item(&finished_entry(3, "gamma.bin", 2048)));
        ui.set_sample_downloads(sort_history.clone().into());

        {
            let sort_history = sort_history.clone();
            let weak = ui.as_weak();
            ui.on_sort_requested(move |column| {
                if let Some(window) = weak.upgrade() {
                    apply_sort_request(&window, &sort_history, column);
                }
            });
        }

        ui.invoke_sort_requested(1);
        assert_eq!(ui.get_sort_column(), 1);
        assert!(ui.get_sort_ascending());
        assert_eq!(
            sort_history
                .iter()
                .map(|item| item.filename.to_string())
                .collect::<Vec<_>>(),
            ["beta.bin", "gamma.bin", "alpha.bin"],
            "Size ascending orders the listed rows"
        );

        ui.invoke_sort_requested(1);
        assert!(!ui.get_sort_ascending(), "the same header flips direction");
        assert_eq!(sort_history.row_data(0).unwrap().filename, "alpha.bin");

        ui.invoke_sort_requested(3);
        assert_eq!(ui.get_sort_column(), 3);
        assert!(ui.get_sort_ascending(), "a new column restarts ascending");
    }

    Ok(())
}

#[test]
fn a_missing_file_counts_as_deleted() {
    use std::io::{Error, ErrorKind};

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

#[test]
fn test_duplicate_prompt_projection_updates_window() -> Result<(), Box<dyn std::error::Error>> {
    let _ = install_test_platform()?;
    let ui = MainWindow::new()?;
    assert!(!ui.get_show_duplicate_dialog());

    let mut snap = crate::engine::DownloadSnapshot {
        session_id: 10,
        url: "http://example.com/file.zip".into(),
        filename: "file.zip".into(),
        status: crate::engine::DownloadStatus::Connecting,
        duplicate: Some(crate::engine::DuplicatePrompt {
            session_id: 10,
            url: "http://example.com/file.zip".into(),
            filename: "file.zip".into(),
            existing_bytes: Some(2048),
            link_duplicate: false,
        }),
        ..Default::default()
    };

    update_window_state(&ui, &snap);
    assert!(ui.get_show_duplicate_dialog());
    assert_eq!(ui.get_duplicate_url(), "http://example.com/file.zip");
    assert_eq!(ui.get_duplicate_filename(), "file.zip");
    assert_eq!(ui.get_duplicate_existing_size(), "2.0 KB");
    assert!(!ui.get_duplicate_is_link());
    assert_eq!(ui.get_duplicate_selected_option(), 0);
    assert!(!ui.get_duplicate_remember());

    ui.set_duplicate_selected_option(1);
    ui.set_duplicate_remember(true);

    // Another update tick for the same prompt preserves user choice
    update_window_state(&ui, &snap);
    assert_eq!(ui.get_duplicate_selected_option(), 1);
    assert!(ui.get_duplicate_remember());

    snap.duplicate = None;
    update_window_state(&ui, &snap);
    assert!(!ui.get_show_duplicate_dialog());

    // Adversarial verification: send_action channel capacity saturation
    let (tx, _rx) = tokio::sync::mpsc::channel(32);
    for _ in 0..32 {
        let ok = send_action(
            &ui,
            &tx,
            crate::engine::DownloadAction::ResolveDuplicate {
                session_id: 1,
                choice: None,
            },
        );
        assert!(ok);
        assert_eq!(ui.get_action_error_message(), "");
    }
    let ok_33 = send_action(
        &ui,
        &tx,
        crate::engine::DownloadAction::ResolveDuplicate {
            session_id: 1,
            choice: None,
        },
    );
    assert!(
        !ok_33,
        "Expected 33rd action to fail due to channel capacity"
    );
    assert_eq!(
        ui.get_action_error_message(),
        "Download engine is busy. Try the action again."
    );

    snap.downloaded_bytes = 1024;
    snap.total_bytes = Some(4096);
    snap.status = crate::engine::DownloadStatus::Downloading;
    update_window_state(&ui, &snap);
    assert_eq!(ui.get_active_size(), "1.0 KB / 4.0 KB");

    snap.status = crate::engine::DownloadStatus::Completed;
    snap.downloaded_bytes = 4096;
    update_window_state(&ui, &snap);
    assert_eq!(ui.get_active_size(), "4.0 KB");

    Ok(())
}

#[test]
fn column_width_defaults_and_save_callback() -> Result<(), Box<dyn std::error::Error>> {
    let _ = install_test_platform()?;
    let ui = MainWindow::new()?;

    assert_eq!(ui.get_col_filename_width(), 294.0);
    assert_eq!(ui.get_col_size_width(), 125.0);
    assert_eq!(ui.get_col_status_width(), 125.0);
    assert_eq!(ui.get_col_time_left_width(), 80.0);
    assert_eq!(ui.get_col_transfer_rate_width(), 90.0);
    assert_eq!(ui.get_col_date_added_width(), 110.0);

    let saved = std::rc::Rc::new(std::cell::Cell::new(false));
    let saved_clone = saved.clone();
    ui.on_save_column_widths(move || {
        saved_clone.set(true);
    });

    ui.set_col_filename_width(420.0);
    assert_eq!(ui.get_col_filename_width(), 420.0);

    ui.invoke_save_column_widths();
    assert!(saved.get());

    Ok(())
}

#[test]
fn completed_download_archiving_preserves_history() {
    use super::TableItem;
    use slint::Model;

    let history = slint::VecModel::<TableItem>::default();
    assert_eq!(history.row_count(), 0);

    let first = finished_entry(1, "file.bin", 1024);
    history.push(history_table_item(&first));
    assert_eq!(history.row_count(), 1);
    let listed = history.row_data(0).unwrap();
    assert_eq!(listed.id, 1, "Row 0 stays reserved for the active download");
    assert_eq!(listed.filename, "file.bin");
    assert_eq!(listed.file_type, "doc");
    assert_eq!(listed.size_text, "1.0 KB");
    assert_eq!(listed.status_text, "Complete");
    assert_eq!(listed.time_left_text, "--:--");
    assert_eq!(listed.transfer_rate_text, "0 KB/s");
    assert_eq!(
        listed.downloaded_text,
        downloaded_label(1_700_000_000_001),
        "The stored completion time is shown, not a fixed label"
    );
    assert_eq!(listed.size_bytes, 1024.0);

    history.push(history_table_item(&finished_entry(2, "archive.ZIP", 2048)));
    assert_eq!(history.row_count(), 2);
    assert_eq!(history.row_data(0).unwrap().id, 1);
    let newest = history.row_data(1).unwrap();
    assert_eq!(newest.id, 2);
    assert_eq!(newest.size_text, "2.0 KB");
    assert_eq!(
        newest.file_type, "zip",
        "Extensions match case-insensitively"
    );
}

#[test]
fn sort_items_orders_by_each_sortable_column() {
    use super::TableItem;

    let mut items: Vec<TableItem> = [
        ("beta.bin", 4096_u64),
        ("Alpha.bin", 1024),
        ("gamma.bin", 2048),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (name, size))| {
        let mut entry = finished_entry(index as i32 + 1, name, size);
        // One minute apart, so the displayed Downloaded times differ.
        entry.completed_unix_ms = 1_700_000_000_000 + index as u64 * 60_000;
        history_table_item(&entry)
    })
    .collect();

    // Name is compared case-insensitively.
    sort_items(&mut items, 0, true);
    let names: Vec<&str> = items.iter().map(|item| item.filename.as_str()).collect();
    assert_eq!(names, ["Alpha.bin", "beta.bin", "gamma.bin"]);

    // Size sorts numerically, and descending flips the order.
    sort_items(&mut items, 1, true);
    assert_eq!(items[0].size_bytes, 1024.0);
    assert_eq!(items[2].size_bytes, 4096.0);
    sort_items(&mut items, 1, false);
    assert_eq!(items[0].size_bytes, 4096.0);

    // Downloaded sorts by the displayed time, oldest first.
    sort_items(&mut items, 3, true);
    assert_eq!(items[0].id, 1);
    assert_eq!(items[2].id, 3);

    // Status and Time left are not sortable, so the order is untouched.
    let order: Vec<i32> = items.iter().map(|item| item.id).collect();
    sort_items(&mut items, 2, true);
    sort_items(&mut items, 5, true);
    assert_eq!(items.iter().map(|item| item.id).collect::<Vec<_>>(), order);

    // Equal keys fall back to ascending id.
    let mut tied: Vec<TableItem> = (1..=2)
        .map(|id| history_table_item(&finished_entry(id, "same.bin", 2048)))
        .collect();
    tied.reverse();
    sort_items(&mut tied, 1, true);
    assert_eq!(tied.iter().map(|item| item.id).collect::<Vec<_>>(), [1, 2]);
}

#[test]
fn history_tracker_reports_each_finish_once() {
    use super::app::HistoryTracker;
    use crate::engine::{DownloadSnapshot, DownloadStatus};

    let finished = DownloadSnapshot {
        session_id: 1,
        url: "https://example.com/file.bin".into(),
        filename: "file.bin".into(),
        save_path: PathBuf::from("file.bin"),
        status: DownloadStatus::Completed,
        total_bytes: Some(1024),
        downloaded_bytes: 1024,
        ..Default::default()
    };
    let next_session = DownloadSnapshot {
        session_id: 2,
        status: DownloadStatus::Connecting,
        ..Default::default()
    };

    // A finish is reported immediately, with the identity the listed row needs.
    let mut tracker = HistoryTracker::new(1);
    let listed = tracker
        .observe(&finished)
        .expect("a finish is reported right away");
    assert_eq!(listed.id, 1, "the first listed row never uses id 0");
    assert_eq!(listed.url, "https://example.com/file.bin");
    assert_eq!(listed.filename, "file.bin");
    assert_eq!(listed.save_path, PathBuf::from("file.bin"));
    assert_eq!(listed.total_bytes, 1024);
    assert!(listed.completed_unix_ms > 0, "the finish time is recorded");

    assert_eq!(
        tracker.observe(&finished),
        None,
        "repeated completed ticks are not listed again"
    );
    assert_eq!(
        tracker.observe(&next_session),
        None,
        "a later session does not re-list the same finish"
    );

    // Each finish is reported once, under its own session.
    let second_finished = DownloadSnapshot {
        session_id: 2,
        filename: "next.bin".into(),
        status: DownloadStatus::Completed,
        total_bytes: Some(2048),
        downloaded_bytes: 2048,
        ..Default::default()
    };
    let listed = tracker
        .observe(&second_finished)
        .expect("the next finish is reported");
    assert_eq!(listed.id, 2, "ids increase, so no listed row is reused");
    assert_eq!(listed.filename, "next.bin");
    assert_eq!(listed.total_bytes, 2048);

    // A finish without a known total still records the bytes it downloaded.
    let mut tracker = HistoryTracker::new(1);
    let listed = tracker
        .observe(&DownloadSnapshot {
            session_id: 7,
            status: DownloadStatus::Completed,
            downloaded_bytes: 512,
            ..Default::default()
        })
        .expect("a finish without a known total is still listed");
    assert_eq!(listed.total_bytes, 512);

    // Kept ids are reserved, so a new row never points at an earlier download.
    let mut tracker = HistoryTracker::new(9);
    let listed = tracker
        .observe(&finished)
        .expect("the download is listed above the kept ids");
    assert_eq!(listed.id, 9, "a new row continues after the kept history");

    // Pausing and resuming starts a new session, but nothing is listed without a finish.
    let mut tracker = HistoryTracker::new(1);
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 5,
            status: DownloadStatus::Downloading,
            ..Default::default()
        }),
        None
    );
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 6,
            status: DownloadStatus::Connecting,
            ..Default::default()
        }),
        None
    );

    // A session that ends idle drops the finish it was holding.
    let mut tracker = HistoryTracker::new(1);
    tracker.observe(&finished);
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 2,
            status: DownloadStatus::Idle,
            ..Default::default()
        }),
        None
    );

    // Overwriting the file also drops it, so stale sizes never reach the list.
    let mut tracker = HistoryTracker::new(1);
    tracker.observe(&finished);
    tracker.clear_completed();
    assert_eq!(tracker.observe(&next_session), None);
}

#[test]
fn completed_download_persists_immediately_without_next_session() {
    use super::app::HistoryTracker;
    use crate::engine::{DownloadSnapshot, DownloadStatus};
    use crate::history::tests::TempFile;

    let file = TempFile::new("immediate-save");
    let mut store = HistoryStore::load_from(file.path().to_path_buf());
    let mut tracker = HistoryTracker::new(store.next_id());

    let finished = DownloadSnapshot {
        session_id: 1,
        url: "https://example.com/testfile.iso".into(),
        filename: "testfile.iso".into(),
        save_path: PathBuf::from("testfile.iso"),
        status: DownloadStatus::Completed,
        total_bytes: Some(1_048_576),
        downloaded_bytes: 1_048_576,
        ..Default::default()
    };

    let entry = tracker
        .observe(&finished)
        .expect("first completed observation emits entry");
    store.record(entry).unwrap();

    // Re-opening the app (loading store afresh from disk) contains the completed download
    let reloaded = HistoryStore::load_from(file.path().to_path_buf());
    assert_eq!(reloaded.entries().len(), 1);
    assert_eq!(reloaded.entries()[0].filename, "testfile.iso");
}
