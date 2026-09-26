use super::support::{
    install_test_platform, option_dir, option_file_types, set_option_dir, set_option_file_types,
};
use crate::ui::options::update_category_defaults;
use crate::ui::save_settings::{Category, SaveSettings};
use crate::ui::view::MainWindow;
use slint::ComponentHandle;
use slint::platform::WindowEvent;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;

#[test]
fn options_startup_checkbox_commits_on_ok() -> Result<(), Box<dyn std::error::Error>> {
    let (window, _clipboard) = install_test_platform()?;
    let ui = MainWindow::new()?;
    ui.show()?;
    window.set_size(slint::PhysicalSize::new(960, 540));

    let commits = Rc::new(RefCell::new(Vec::new()));
    ui.on_commit_options({
        let commits = commits.clone();
        move |enabled| commits.borrow_mut().push(enabled)
    });

    let render = || {
        window.draw_if_needed(|renderer| {
            let mut pixels = vec![slint::Rgb8Pixel::default(); 960 * 540];
            renderer.render(&mut pixels, 960);
        });
    };
    let press = |text: slint::SharedString| {
        window.dispatch_event(WindowEvent::KeyPressed { text });
    };

    ui.set_startup_option_visible(true);
    ui.set_show_options_dialog(true);
    render();

    press(slint::platform::Key::Tab.into());
    press(slint::platform::Key::Tab.into());
    press(" ".into());
    assert!(ui.get_options_launch_on_startup());
    press(slint::platform::Key::Tab.into());
    press(slint::platform::Key::Return.into());
    assert!(!ui.get_show_options_dialog());
    assert_eq!(*commits.borrow(), vec![true], "OK commits the change");

    ui.set_show_options_dialog(true);
    render();
    press(slint::platform::Key::Tab.into());
    press(slint::platform::Key::Tab.into());
    press(" ".into());
    assert!(
        !ui.get_options_launch_on_startup(),
        "the checkbox toggles back off"
    );
    press(slint::platform::Key::Escape.into());
    assert!(!ui.get_show_options_dialog());
    assert_eq!(*commits.borrow(), vec![true], "Escape discards the change");

    Ok(())
}

#[test]
fn options_save_to_tab_properties_and_navigation() -> Result<(), Box<dyn std::error::Error>> {
    let (window, _clipboard) = install_test_platform()?;
    let ui = MainWindow::new()?;
    ui.show()?;
    window.set_size(slint::PhysicalSize::new(960, 540));

    let render = || {
        window.draw_if_needed(|renderer| {
            let mut pixels = vec![slint::Rgb8Pixel::default(); 960 * 540];
            renderer.render(&mut pixels, 960);
        });
    };
    let press = |text: slint::SharedString| {
        window.dispatch_event(WindowEvent::KeyPressed { text });
    };

    ui.set_options_selected_tab(2); // Save To tab
    set_option_dir(&ui, 0, "D:\\Downloads");
    set_option_dir(&ui, 1, "D:\\Downloads\\Compressed");
    set_option_dir(&ui, 5, "D:\\Downloads\\Video");
    ui.set_show_options_dialog(true);
    render();

    assert_eq!(ui.get_options_selected_tab(), 2);
    assert_eq!(ui.get_options_save_category(), 0);
    assert_eq!(option_dir(&ui, 0), "D:\\Downloads");

    // Cycle through categories on the Save To tab
    ui.set_options_save_category(1); // Compressed
    assert_eq!(ui.get_options_save_category(), 1);
    assert_eq!(option_dir(&ui, 1), "D:\\Downloads\\Compressed");

    ui.set_options_save_category(5); // Video
    assert_eq!(ui.get_options_save_category(), 5);
    assert_eq!(option_dir(&ui, 5), "D:\\Downloads\\Video");

    // Test reset default calculation
    let default_dir = PathBuf::from("D:\\Downloads");
    let video_sub = SaveSettings::default_subfolder(&default_dir, Category::Video);
    assert_eq!(video_sub, PathBuf::from("D:\\Downloads\\Video"));

    let compressed_sub = SaveSettings::default_subfolder(&default_dir, Category::Compressed);
    assert_eq!(compressed_sub, PathBuf::from("D:\\Downloads\\Compressed"));

    // Verify pointer clicks do not leave focus rings on Save To controls
    let click = |x: f32, y: f32| {
        window.dispatch_event(WindowEvent::PointerMoved {
            position: slint::LogicalPosition::new(x, y),
        });
        window.dispatch_event(WindowEvent::PointerPressed {
            position: slint::LogicalPosition::new(x, y),
            button: slint::platform::PointerEventButton::Left,
        });
        window.dispatch_event(WindowEvent::PointerReleased {
            position: slint::LogicalPosition::new(x, y),
            button: slint::platform::PointerEventButton::Left,
        });
        render();
    };
    let capture = || {
        window.request_redraw();
        let mut probe = vec![slint::Rgb8Pixel::default(); (960 * 540) as usize];
        window.draw_if_needed(|renderer| {
            renderer.render(&mut probe, 960);
        });
        probe
    };
    let pixel =
        |probe: &[slint::Rgb8Pixel], x: f32, y: f32| probe[(y as usize) * 960 + (x as usize)];
    let ring = slint::Rgb8Pixel {
        r: 0x8b,
        g: 0xa9,
        b: 0xd6,
    };

    // Click Default button (centered around x = 692, y = 281)
    click(692.0, 281.0);
    let probe = capture();
    assert_ne!(
        pixel(&probe, 692.0, 261.0),
        ring,
        "Clicking Default button with mouse does not show focus ring"
    );

    // Click Category combobox (x = 414, y = 208) to open, then click outside (x = 210, y = 90) to close
    click(414.0, 208.0);
    click(210.0, 90.0);
    let probe = capture();
    assert_ne!(
        pixel(&probe, 414.0, 192.0),
        ring,
        "Opening and closing Category combobox with mouse does not show focus ring"
    );

    // Reopen dialog to test keyboard navigation from fresh state
    press(slint::platform::Key::Escape.into());
    assert!(!ui.get_show_options_dialog());
    ui.set_show_options_dialog(true);
    render();

    press(slint::platform::Key::Tab.into());
    press(slint::platform::Key::Tab.into());
    let probe = capture();
    assert_eq!(
        pixel(&probe, 414.0, 192.0),
        ring,
        "Tab from tab strip moves focus to Category combobox and shows focus ring"
    );

    press(slint::platform::Key::Escape.into());
    assert!(!ui.get_show_options_dialog());

    Ok(())
}

#[test]
fn add_download_category_routing_on_url_change() {
    let mut settings = SaveSettings {
        default_dir: PathBuf::from("C:\\Users\\user\\Downloads"),
        ..Default::default()
    };
    settings.set_category_dir(
        Category::Compressed,
        Some(PathBuf::from("C:\\Users\\user\\Downloads\\Archives")),
    );
    settings.set_category_dir(Category::Video, Some(PathBuf::from("E:\\Media\\Videos")));

    assert_eq!(
        settings.category_path(Category::General),
        PathBuf::from("C:\\Users\\user\\Downloads")
    );
    assert_eq!(
        settings.category_path(Category::Compressed),
        PathBuf::from("C:\\Users\\user\\Downloads\\Archives")
    );
    assert_eq!(
        settings.category_path(Category::Video),
        PathBuf::from("E:\\Media\\Videos")
    );
    assert_eq!(
        settings.category_path(Category::Documents),
        PathBuf::from("C:\\Users\\user\\Downloads\\Documents")
    );

    assert_eq!(
        settings.path_for_url("https://example.com/download.zip"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Archives")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/download.tar.gz"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Archives")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/movie.mkv"),
        PathBuf::from("E:\\Media\\Videos")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/song.flac"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Music")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/doc.pdf"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Documents")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/installer.msi"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Programs")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/api/data?format=raw"),
        PathBuf::from("C:\\Users\\user\\Downloads")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/data.tar.bz2"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Archives")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/audio.opus"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Music")
    );
    assert_eq!(
        settings.path_for_url("https://example.com/sheet.csv"),
        PathBuf::from("C:\\Users\\user\\Downloads\\Documents")
    );
}

#[test]
fn options_category_defaults_cascade_on_default_dir_change() {
    let ui = MainWindow::new().unwrap();
    let old_def = PathBuf::from("C:\\Users\\user\\Downloads");
    let new_def = PathBuf::from("D:\\Downloads");

    set_option_dir(&ui, 0, &old_def.to_string_lossy());
    set_option_dir(
        &ui,
        1,
        &SaveSettings::default_subfolder(&old_def, Category::Compressed).to_string_lossy(),
    );
    set_option_dir(
        &ui,
        2,
        &SaveSettings::default_subfolder(&old_def, Category::Documents).to_string_lossy(),
    );
    set_option_dir(&ui, 3, "E:\\CustomMusic");
    set_option_dir(
        &ui,
        4,
        &SaveSettings::default_subfolder(&old_def, Category::Programs).to_string_lossy(),
    );
    set_option_dir(
        &ui,
        5,
        &SaveSettings::default_subfolder(&old_def, Category::Video).to_string_lossy(),
    );

    update_category_defaults(&ui, &old_def, &new_def);
    set_option_dir(&ui, 0, &new_def.to_string_lossy());

    assert_eq!(
        option_dir(&ui, 1),
        SaveSettings::default_subfolder(&new_def, Category::Compressed)
            .to_string_lossy()
            .into_owned()
    );
    assert_eq!(
        option_dir(&ui, 2),
        SaveSettings::default_subfolder(&new_def, Category::Documents)
            .to_string_lossy()
            .into_owned()
    );
    assert_eq!(
        option_dir(&ui, 4),
        SaveSettings::default_subfolder(&new_def, Category::Programs)
            .to_string_lossy()
            .into_owned()
    );
    assert_eq!(
        option_dir(&ui, 5),
        SaveSettings::default_subfolder(&new_def, Category::Video)
            .to_string_lossy()
            .into_owned()
    );

    assert_eq!(option_dir(&ui, 3), "E:\\CustomMusic");
}

#[test]
fn routed_categories_agree_with_history_file_types() {
    let settings = SaveSettings::default();
    for category in Category::ALL {
        for extension in category.extensions() {
            let filename = format!("sample.{extension}");
            assert_eq!(
                settings.category_for_filename(&filename),
                category,
                "{filename} routed to {category:?} must match its listed file type"
            );
        }
    }

    assert_eq!(
        settings.category_for_filename("sample.unknown"),
        Category::General,
        "Extensions outside the classifier fall back to the default directory"
    );

    let mut custom = SaveSettings::default();
    custom
        .file_types
        .insert(Category::Music, vec!["mod".into()]);
    assert_eq!(
        custom.category_for_filename("track.mod"),
        Category::Music,
        "A custom extension classifies as its edited category"
    );
}

#[test]
fn built_in_file_type_lists_are_verbose_and_disjoint() {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for category in Category::ALL {
        let extensions = category.extensions();
        if category == Category::General {
            assert!(extensions.is_empty(), "General catches everything else");
            continue;
        }
        let minimum = if category == Category::Torrents {
            3
        } else {
            30
        };
        assert!(
            extensions.len() >= minimum,
            "{category:?} lists {} extensions, expected at least {minimum}",
            extensions.len()
        );
        for extension in extensions {
            assert_eq!(
                *extension,
                extension.to_ascii_lowercase().as_str(),
                "{extension} must be stored lowercase"
            );
            assert!(
                seen.insert(*extension),
                "{extension} is listed in more than one category"
            );
        }
    }
}

#[test]
fn new_categories_route_their_representative_extensions() {
    let settings = SaveSettings::default();
    for (filename, expected) in [
        ("disk.iso", Category::DiskImages),
        ("book.epub", Category::Ebooks),
        ("photo.heic", Category::Images),
        ("main.rs", Category::SourceCode),
        ("ubuntu.torrent", Category::Torrents),
        ("data.sqlite", Category::Databases),
        ("report.doc", Category::Documents),
        ("archive.7z", Category::Compressed),
    ] {
        assert_eq!(
            settings.category_for_filename(filename),
            expected,
            "{filename} must route to {expected:?}"
        );
    }

    assert_eq!(
        settings.path_for_url("https://example.com/disk.iso"),
        settings.category_path(Category::DiskImages)
    );
    assert!(
        settings
            .category_path(Category::SourceCode)
            .ends_with("Source Code"),
        "New categories use their display subfolder"
    );
}

#[test]
fn options_category_switch_preserves_other_category_dirs() -> Result<(), Box<dyn std::error::Error>>
{
    let (window, _clipboard) = install_test_platform()?;
    let ui = MainWindow::new()?;
    ui.show()?;
    window.set_size(slint::PhysicalSize::new(960, 540));

    let render = || {
        window.draw_if_needed(|renderer| {
            let mut pixels = vec![slint::Rgb8Pixel::default(); 960 * 540];
            renderer.render(&mut pixels, 960);
        });
    };

    let names: Vec<slint::SharedString> = Category::ALL
        .iter()
        .map(|category| category.display_name().into())
        .collect();
    ui.set_options_category_names(Rc::new(slint::VecModel::from(names)).into());
    ui.set_options_selected_tab(2);
    set_option_dir(&ui, 0, "D:\\Downloads");
    set_option_dir(&ui, 1, "D:\\Downloads\\Compressed");
    ui.set_show_options_dialog(true);
    render();

    let key = |text: slint::SharedString| window.dispatch_event(WindowEvent::KeyPressed { text });
    key(slint::platform::Key::Tab.into());
    key(slint::platform::Key::Tab.into());
    key(slint::platform::Key::DownArrow.into());

    assert_eq!(ui.get_options_save_category(), 1);
    assert_eq!(
        option_dir(&ui, 1),
        "D:\\Downloads\\Compressed",
        "Switching to a category must not copy the previous category's path into it"
    );

    Ok(())
}

#[test]
fn options_file_types_edit_each_category_separately() -> Result<(), Box<dyn std::error::Error>> {
    use slint::platform::Key;

    let (window, _clipboard) = install_test_platform()?;
    let ui = MainWindow::new()?;
    ui.show()?;
    window.set_size(slint::PhysicalSize::new(960, 540));

    let names: Vec<slint::SharedString> = Category::ALL
        .iter()
        .map(|category| category.display_name().into())
        .collect();
    ui.set_options_category_names(Rc::new(slint::VecModel::from(names)).into());
    ui.set_options_selected_tab(2);
    set_option_file_types(&ui, 1, "zip, rar");
    set_option_file_types(&ui, 5, "mp4");
    ui.set_show_options_dialog(true);

    let key = |text: slint::SharedString| window.dispatch_event(WindowEvent::KeyPressed { text });
    let replace_text = |text: &str| {
        window.dispatch_event(WindowEvent::KeyPressed {
            text: Key::Control.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed { text: "a".into() });
        window.dispatch_event(WindowEvent::KeyReleased { text: "a".into() });
        window.dispatch_event(WindowEvent::KeyReleased {
            text: Key::Control.into(),
        });
        window.dispatch_event(WindowEvent::KeyPressed { text: text.into() });
    };

    key(Key::Tab.into());
    key(Key::Tab.into());
    key(Key::DownArrow.into());
    assert_eq!(ui.get_options_save_category(), 1);
    for _ in 0..4 {
        key(Key::Tab.into());
    }

    replace_text("7z, tar");
    assert_eq!(
        option_file_types(&ui, 1),
        "7z, tar",
        "Typing in the file types field edits the selected category"
    );
    assert_eq!(
        option_file_types(&ui, 5),
        "mp4",
        "Other categories keep their own file types"
    );

    ui.set_options_save_category(5);
    replace_text("webm");
    assert_eq!(option_file_types(&ui, 5), "webm");
    assert_eq!(
        option_file_types(&ui, 1),
        "7z, tar",
        "Switching categories keeps the edited list"
    );

    Ok(())
}

#[test]
fn options_file_types_refresh_when_props_reset_after_open() -> Result<(), Box<dyn std::error::Error>>
{
    use slint::platform::Key;

    let (window, _clipboard) = install_test_platform()?;
    let ui = MainWindow::new()?;
    ui.show()?;
    window.set_size(slint::PhysicalSize::new(960, 540));

    ui.set_options_selected_tab(2);
    ui.set_options_save_category(1);
    set_option_file_types(&ui, 1, "zip");
    ui.set_show_options_dialog(true);
    window.draw_if_needed(|renderer| {
        let mut pixels = vec![slint::Rgb8Pixel::default(); 960 * 540];
        renderer.render(&mut pixels, 960);
    });

    set_option_file_types(&ui, 1, "png");

    let key = |text: slint::SharedString| window.dispatch_event(WindowEvent::KeyPressed { text });
    key(Key::Tab.into());
    key(Key::Tab.into());
    for _ in 0..4 {
        key(Key::Tab.into());
    }
    key("!".into());

    let value = option_file_types(&ui, 1).to_string();
    assert!(
        value.contains('!'),
        "the keystroke must reach the file types field, got {value:?}"
    );
    assert!(
        value.contains("png"),
        "the field must show the refreshed list, got {value:?}"
    );
    assert!(
        !value.contains("zip"),
        "the field must not keep the pre-reset text, got {value:?}"
    );

    Ok(())
}
