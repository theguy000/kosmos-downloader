use crate::history::HistoryEntry;
use crate::ui::view::MainWindow;
use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
use slint::platform::{Clipboard, Platform, WindowAdapter};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

pub(super) fn finished_entry(id: i32, filename: &str, total_bytes: u64) -> HistoryEntry {
    HistoryEntry {
        id,
        url: format!("https://example.com/{filename}"),
        filename: filename.to_string(),
        save_path: PathBuf::from(filename),
        total_bytes,
        completed_unix_ms: 1_700_000_000_000 + id as u64,
    }
}

pub(super) fn replace_category_value(
    model: &slint::ModelRc<slint::SharedString>,
    category: usize,
    value: &str,
) -> slint::ModelRc<slint::SharedString> {
    use slint::Model;
    let mut values: Vec<slint::SharedString> = model.iter().collect();
    if let Some(slot) = values.get_mut(category) {
        *slot = value.into();
    }
    slint::ModelRc::from(values.as_slice())
}

pub(super) fn option_dir(ui: &MainWindow, category: usize) -> slint::SharedString {
    use slint::Model;
    ui.get_options_category_dirs()
        .row_data(category)
        .unwrap_or_default()
}

pub(super) fn set_option_dir(ui: &MainWindow, category: usize, value: &str) {
    let model = replace_category_value(&ui.get_options_category_dirs(), category, value);
    ui.set_options_category_dirs(model);
}

pub(super) fn option_file_types(ui: &MainWindow, category: usize) -> slint::SharedString {
    use slint::Model;
    ui.get_options_category_file_types()
        .row_data(category)
        .unwrap_or_default()
}

pub(super) fn set_option_file_types(ui: &MainWindow, category: usize, value: &str) {
    let model = replace_category_value(&ui.get_options_category_file_types(), category, value);
    ui.set_options_category_file_types(model);
}

pub(super) type TestContext = (Rc<MinimalSoftwareWindow>, Rc<RefCell<String>>);

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
pub(super) fn install_test_platform() -> Result<TestContext, Box<dyn std::error::Error>> {
    if let Some((window, clipboard)) = TEST_CONTEXT.with(|slot| slot.borrow().clone()) {
        return Ok((window, clipboard));
    }
    let window = MinimalSoftwareWindow::new(RepaintBufferType::ReusedBuffer);
    let clipboard = Rc::new(RefCell::new(String::new()));
    slint::platform::set_platform(Box::new(TestPlatform(window.clone(), clipboard.clone())))?;
    TEST_CONTEXT.with(|slot| *slot.borrow_mut() = Some((window.clone(), clipboard.clone())));
    Ok((window, clipboard))
}
