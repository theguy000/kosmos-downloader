slint::slint! {
    import { Slider } from "std-widgets.slint";

    export struct TableItem {
        id: int,
        filename: string,
        file_type: string,
        size_text: string,
        status_text: string,
        time_left_text: string,
        transfer_rate_text: string,
        last_try_text: string,
    }

    // --- Refined Minimalist Vector Icons (Zero Emojis, Sleek Line Art) ---

    component IconAdd inherits Path {
        width: 16px; height: 16px;
        commands: "M 8 2 L 8 14 M 2 8 L 14 8";
        stroke: #10b981;
        stroke-width: 1.5px;
    }

    component IconResume inherits Path {
        width: 16px; height: 16px;
        commands: "M 5 3 L 13 8 L 5 13 Z";
        stroke: #10b981;
        fill: #10b98133;
        stroke-width: 1.2px;
    }

    component IconStop inherits Path {
        width: 16px; height: 16px;
        commands: "M 4 4 L 12 4 L 12 12 L 4 12 Z";
        stroke: #f59e0b;
        fill: #f59e0b20;
        stroke-width: 1.4px;
    }

    component IconStopAll inherits Path {
        width: 16px; height: 16px;
        commands: "M 2 6 L 10 6 L 10 14 L 2 14 Z M 6 2 L 14 2 L 14 10";
        stroke: #f59e0b;
        fill: #f59e0b20;
        stroke-width: 1.4px;
    }

    component IconDelete inherits Path {
        width: 16px; height: 16px;
        commands: "M 2.5 4.5 L 13.5 4.5 M 6 2.5 L 10 2.5 M 4 4.5 L 4.8 13.5 A 1 1 0 0 0 5.8 14.5 L 10.2 14.5 A 1 1 0 0 0 11.2 13.5 L 12 4.5 M 6.5 6.5 L 6.5 12 M 9.5 6.5 L 9.5 12";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconDeleteCompleted inherits Path {
        width: 16px; height: 16px;
        commands: "M 2 4 L 10 4 M 4.5 2 L 7.5 2 M 3 4 L 3.8 13.2 A 1 1 0 0 0 4.8 14 L 8.5 14 M 9.5 10 L 11.5 12.5 L 15.5 7";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconOptions inherits Path {
        width: 16px; height: 16px;
        commands: "M 2 4 L 14 4 M 5 2 L 5 6 M 2 8 L 14 8 M 11 6 L 11 10 M 2 12 L 14 12 M 7 10 L 7 14";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }


    component IconChevronDown inherits Path {
        width: 8px; height: 8px;
        commands: "M 1 2.5 L 4 5.5 L 7 2.5";
        stroke: #71717a;
        stroke-width: 1.2px;
    }

    component IconChevronRight inherits Path {
        width: 8px; height: 8px;
        commands: "M 2.5 1 L 5.5 4 L 2.5 7";
        stroke: #71717a;
        stroke-width: 1.2px;
    }

    component IconFolder inherits Path {
        width: 13px; height: 13px;
        commands: "M 1.5 11 L 1.5 2 L 5 2 L 6.5 3.5 L 10.5 3.5 L 10.5 5.5 M 1.5 11 L 3.5 5.5 L 12 5.5 L 10 11 Z";
        stroke: #a1a1aa;
        stroke-width: 1.2px;
    }

    component IconDoc inherits Path {
        width: 13px; height: 13px;
        commands: "M 2 1.5 L 7.5 1.5 L 11 5 L 11 11.5 L 2 11.5 Z M 7.5 1.5 L 7.5 5 L 11 5 M 4 7 L 8.5 7 M 4 9 L 7 9";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconMedia inherits Path {
        width: 13px; height: 13px;
        commands: "M 1 3 L 8.5 3 L 8.5 10 L 1 10 Z M 8.5 5 L 12 3 L 12 10 L 8.5 8";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconArchive inherits Path {
        width: 13px; height: 13px;
        commands: "M 2 1.5 L 11 1.5 L 11 11.5 L 2 11.5 Z M 5 1.5 L 5 3 M 5 4.5 L 7 4.5 M 5 6.5 L 7 6.5 M 5 8.5 L 7 8.5 L 7 10 L 5 10 Z";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconProgram inherits Path {
        width: 13px; height: 13px;
        commands: "M 1 2 L 12 2 L 12 11 L 1 11 Z M 1 4 L 12 4 M 3 6 L 5 7.5 L 3 9 M 7 9 L 10 9";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconAudio inherits Path {
        width: 13px; height: 13px;
        commands: "M 4.5 9.5 L 4.5 3 L 11 1.5 L 11 8 M 4.5 5 L 11 3.5 M 4.5 9.5 A 1.75 1.5 0 1 1 1 9.5 A 1.75 1.5 0 1 1 4.5 9.5 Z M 11 8 A 1.75 1.5 0 1 1 7.5 8 A 1.75 1.5 0 1 1 11 8 Z";
        stroke: #9ca3af;
        stroke-width: 1.2px;
    }

    component IconCheck inherits Path {
        width: 13px; height: 13px;
        commands: "M 3 4.5 L 5.5 7 L 10 2 M 1.5 8.5 L 1.5 11.5 L 11.5 11.5 L 11.5 8.5";
        stroke: #10b981;
        stroke-width: 1.2px;
    }

    component IconUnfinished inherits Path {
        width: 13px; height: 13px;
        commands: "M 6.5 1 L 6.5 8 M 3.5 5 L 6.5 8 L 9.5 5 M 1.5 8.5 L 1.5 11.5 L 11.5 11.5 L 11.5 8.5";
        stroke: #f59e0b;
        stroke-width: 1.2px;
    }

    component IconGrabber inherits Path {
        width: 13px; height: 13px;
        commands: "M 6.5 1 A 5.5 5.5 0 1 1 6.5 12 A 5.5 5.5 0 1 1 6.5 1 Z M 6.5 1 C 3.5 4 3.5 9 6.5 12 C 9.5 9 9.5 4 6.5 1 M 1 6.5 L 12 6.5";
        stroke: #a1a1aa;
        stroke-width: 1.2px;
    }

    component IconQueue inherits Path {
        width: 13px; height: 13px;
        commands: "M 1 2.5 L 12 2.5 M 1 6.5 L 6 6.5 M 1 10.5 L 6 10.5 M 8.5 5.5 L 12 8 L 8.5 10.5 Z";
        stroke: #a1a1aa;
        stroke-width: 1.2px;
    }

    component FileTypeIcon inherits VerticalLayout {
        in property <string> file-type;
        alignment: center;

        if root.file-type == "exe": IconProgram {}
        if root.file-type == "zip": IconArchive {}
        if root.file-type == "video": IconMedia {}
        if root.file-type == "audio": IconAudio {}
        if root.file-type == "doc": IconDoc {}
    }

    component DownloadRowColumns inherits HorizontalLayout {
        in property <string> filename;
        in property <string> file-type;
        in property <string> size-text;
        in property <string> status-text;
        in property <string> time-left-text;
        in property <string> transfer-rate-text;
        in property <string> last-try-text;
        in property <bool> is-active: false;
        in property <color> status-color: #10b981;

        padding-left: 8px;
        padding-right: 8px;
        spacing: 0px;

        HorizontalLayout {
            horizontal-stretch: 3;
            spacing: 6px;
            alignment: start;
            FileTypeIcon { file-type: root.file-type; }
            Text {
                text: root.filename;
                font-size: 11px;
                color: root.is-active ? #f4f4f5 : #d4d4d8;
                overflow: elide;
                vertical-alignment: center;
            }
        }

        HorizontalLayout {
            width: 80px;
            padding-left: 6px;
            alignment: start;
            Text {
                text: root.size-text;
                font-size: 11px;
                color: root.is-active ? #d4d4d8 : #71717a;
                vertical-alignment: center;
            }
        }

        HorizontalLayout {
            width: 95px;
            padding-left: 6px;
            alignment: start;
            Text {
                text: root.status-text;
                font-size: 11px;
                color: root.status-color;
                vertical-alignment: center;
            }
        }

        HorizontalLayout {
            width: 80px;
            padding-left: 6px;
            alignment: start;
            Text {
                text: root.time-left-text;
                font-size: 11px;
                color: root.is-active ? #d4d4d8 : #52525b;
                vertical-alignment: center;
            }
        }

        HorizontalLayout {
            width: 90px;
            padding-left: 6px;
            alignment: start;
            Text {
                text: root.transfer-rate-text;
                font-size: 11px;
                color: root.is-active ? #d4d4d8 : #52525b;
                vertical-alignment: center;
            }
        }

        HorizontalLayout {
            width: 90px;
            padding-left: 6px;
            alignment: start;
            Text {
                text: root.last-try-text;
                font-size: 11px;
                color: root.is-active ? #71717a : #52525b;
                vertical-alignment: center;
            }
        }
    }

    component CategoryRow inherits Rectangle {
        in property <string> text;
        in property <bool> enabled: true;
        in property <bool> selected;
        in property <bool> child;
        in property <bool> last-child;
        in property <bool> expandable;
        in property <bool> expanded;
        callback clicked();
        callback toggle();
        callback activate();
        activate => {
            if root.enabled {
                if root.expandable && root.selected { root.toggle(); }
                root.clicked();
            }
        }

        height: 26px;
        background: !root.enabled ? transparent : (root.selected ? #273248 : (touch.has-hover ? #242428 : transparent));
        border-width: root.enabled && focus.has-focus ? 1px : 0px;
        border-color: #8ba9d6;
        forward-focus: focus;
        accessible-role: button;
        accessible-label: root.enabled ? root.text : root.text + " (unavailable)";
        accessible-enabled: root.enabled;
        accessible-action-default => { root.activate(); }

        if root.child: Rectangle {
            x: 30px; y: 0px;
            width: 1px;
            height: root.last-child ? parent.height / 2 : parent.height;
            background: #34343b;
        }
        if root.child: Rectangle {
            x: 30px; y: parent.height / 2;
            width: 9px; height: 1px;
            background: #34343b;
        }

        Rectangle {
            x: root.child ? 44px : 24px;
            y: (parent.height - self.height) / 2;
            width: 16px; height: 16px;
            opacity: root.enabled ? 1 : 0.3;
            HorizontalLayout {
                alignment: center;
                @children
            }
        }
        Text {
            x: root.child ? 66px : 46px;
            width: parent.width - self.x - 8px;
            height: parent.height;
            text: root.text;
            font-size: 12px;
            font-weight: 400;
            color: !root.enabled ? #71717a : (root.selected ? #edf3ff : #c4c4cc);
            vertical-alignment: center;
            overflow: elide;
        }

        focus := FocusScope {
            enabled: root.enabled;
            key-pressed(event) => {
                if event.text == " " || event.text == "\n" {
                    root.activate();
                    return accept;
                }
                if root.expandable && ((event.text == Key.RightArrow && !root.expanded)
                    || (event.text == Key.LeftArrow && root.expanded)) {
                    root.toggle();
                    return accept;
                }
                return reject;
            }
        }
        // Hit areas overlay the content; they must not participate in its layout.
        touch := TouchArea {
            enabled: root.enabled;
            mouse-cursor: root.enabled ? pointer : default;
            clicked => {
                focus.focus();
                root.activate();
            }
        }
        if root.expandable: Rectangle {
            x: 0px;
            width: 24px; height: parent.height;
            background: disclosure.has-hover ? #ffffff0c : transparent;
            if root.expanded: IconChevronDown { x: 8px; y: 9px; stroke: #a1a1aa; }
            if !root.expanded: IconChevronRight { x: 8px; y: 9px; stroke: #a1a1aa; }
            disclosure := TouchArea {
                enabled: root.enabled;
                mouse-cursor: pointer;
                clicked => { focus.focus(); root.toggle(); }
            }
        }
    }

    // --- Minimal Toolbar Button ---

    component IdmToolButton inherits Rectangle {
        in property <string> text: "";
        in property <bool> enabled: true;
        callback clicked();

        width: 62px;
        height: 48px;
        background: !root.enabled ? transparent : (touch.pressed ? #34343b :
            (touch.has-hover ? #2b2b30 : transparent));
        border-width: root.enabled && focus.has-focus ? 1px : 0px;
        border-color: #8ba9d6;
        forward-focus: focus;
        accessible-role: button;
        accessible-label: root.text;
        accessible-enabled: root.enabled;
        accessible-action-default => { if root.enabled { root.clicked(); } }

        VerticalLayout {
            alignment: center;
            spacing: 3px;
            padding: 2px;

            Rectangle {
                height: 16px;
                opacity: root.enabled ? 1 : 0.3;
                HorizontalLayout {
                    alignment: center;
                    @children
                }
            }

            Text {
                text: root.text;
                font-size: 10px;
                color: !root.enabled ? #71717a : (touch.has-hover ? #f4f4f5 : #d4d4d8);
                horizontal-alignment: center;
                overflow: elide;
            }
        }
        focus := FocusScope {
            enabled: root.enabled;
            key-pressed(event) => {
                if root.enabled && (event.text == Key.Space || event.text == Key.Return) {
                    root.clicked();
                    return accept;
                }
                return reject;
            }
        }
        touch := TouchArea {
            enabled: root.enabled;
            mouse-cursor: root.enabled ? pointer : default;
            clicked => { focus.focus(); root.clicked(); }
        }
    }

    component PointerButton inherits Rectangle {
        in property <string> text;
        in property <bool> primary: false;
        in property <bool> enabled: true;
        callback clicked();

        width: root.text == "Download" ? 88px : 72px;
        height: 36px;
        background: !root.enabled ? #303034 : (touch.pressed ? #3b4658 :
            (root.primary ? (touch.has-hover ? #a7bfe2 : #8ba9d6) :
            (touch.has-hover ? #303036 : transparent)));
        border-width: focus.has-focus ? 1px : 0px;
        border-color: #edf3ff;
        forward-focus: focus;
        accessible-role: button;
        accessible-label: root.text;
        accessible-enabled: root.enabled;
        accessible-action-default => { if root.enabled { root.clicked(); } }

        Text {
            text: root.text;
            font-size: 12px;
            font-weight: root.primary ? 600 : 400;
            color: !root.enabled ? #92929b : (root.primary && !touch.pressed ? #18181b : #f4f4f5);
            horizontal-alignment: center;
            vertical-alignment: center;
        }
        focus := FocusScope {
            enabled: root.enabled;
            key-pressed(event) => {
                if event.text == Key.Space || event.text == Key.Return {
                    root.clicked();
                    return accept;
                }
                return reject;
            }
        }
        touch := TouchArea {
            enabled: root.enabled;
            mouse-cursor: root.enabled ? pointer : default;
            clicked => { focus.focus(); root.clicked(); }
        }
    }

    component DialogInput inherits Rectangle {
        in-out property <string> text;
        in property <string> placeholder-text;
        in property <string> label;

        height: 36px;
        horizontal-stretch: 1;
        background: input.has-focus ? #303036 : #2b2b30;
        forward-focus: input;
        TouchArea { clicked => { input.focus(); } }
        if root.text == "": Text {
            x: 12px;
            width: parent.width - 24px;
            height: parent.height;
            text: root.placeholder-text;
            color: #a1a1aa;
            font-size: 12px;
            vertical-alignment: center;
            overflow: elide;
        }
        input := TextInput {
            x: 12px;
            width: parent.width - 24px;
            height: parent.height;
            text <=> root.text;
            single-line: true;
            font-size: 12px;
            color: #f4f4f5;
            selection-background-color: #8ba9d6;
            selection-foreground-color: #18181b;
            vertical-alignment: center;
            accessible-label: root.label;
        }
        Rectangle {
            y: parent.height - 2px;
            height: 2px;
            background: input.has-focus ? #8ba9d6 : transparent;
        }
    }

    component DialogSlider inherits Rectangle {
        in-out property <float> value;
        height: 28px;
        horizontal-stretch: 1;
        forward-focus: slider;

        Rectangle {
            x: 10px; y: (parent.height - self.height) / 2;
            width: parent.width - 20px; height: 3px;
            background: #45454d;
            Rectangle {
                x: 0px;
                width: parent.width * (root.value - 1) / 15;
                background: #8ba9d6;
            }
        }
        Rectangle {
            x: 4px + (parent.width - 20px) * (root.value - 1) / 15;
            y: (parent.height - self.height) / 2;
            width: 12px; height: 16px;
            background: slider.has-focus ? #edf3ff : #8ba9d6;
        }
        // ponytail: keep native slider input and accessibility; replace only its paint.
        slider := Slider {
            width: parent.width; height: parent.height;
            opacity: 0;
            minimum: 1; maximum: 16; step: 1;
            value <=> root.value;
            changed(value) => { root.value = Math.round(value); }
            accessible-label: "Streams";
        }
    }

    // --- Main Window Component ---

    export component MainWindow inherits Window {
        title: "Kosmos Downloader";
        preferred-width: 960px;
        preferred-height: 540px;
        min-width: 760px;
        min-height: 420px;
        background: #18181b;

        in-out property <string> url_text: "";
        in-out property <string> dest_dir_text: "";
        in-out property <float> streams_count: 8;
        in-out property <bool> show_add_dialog: false;
        in-out property <int> selected_category: 0;
        in-out property <int> selected_row: 0;

        // Tree Expansion States
        in-out property <bool> all_downloads_expanded: true;

        in-out property <bool> has_active_download: false;
        in-out property <bool> is_downloading: false;
        in-out property <bool> is_paused: false;
        in-out property <bool> is_completed: false;
        in-out property <bool> is_resumable: false;
        in-out property <string> active_filename: "";
        in-out property <string> active_file_type: "doc";
        in-out property <string> active_size: "0 B";
        in-out property <string> active_status: "Idle";
        in-out property <color> active_status_color: #71717a;
        in-out property <string> active_time_left: "--:--";
        in-out property <string> active_transfer_rate: "0 KB/s";
        in-out property <string> active_last_try: "Today";
        in-out property <string> active_error_message: "";
        in-out property <string> action_error_message: "";
        pure function category_matches(category: int, file_type: string, completed: bool) -> bool {
            return category == 0
                || (category == 1 && file_type == "zip")
                || (category == 2 && file_type == "doc")
                || (category == 3 && file_type == "audio")
                || (category == 4 && file_type == "exe")
                || (category == 5 && file_type == "video")
                || (category == 6 && !completed)
                || (category == 7 && completed);
        }
        out property <bool> active_row_visible: root.has_active_download && root.category_matches(
            root.selected_category, root.active_file_type, root.is_completed);
        out property <bool> can_resume: root.active_row_visible && root.selected_row == 0 && root.is_paused;
        out property <bool> can_stop: root.active_row_visible && root.selected_row == 0 && root.is_downloading;
        out property <bool> can_stop_all: root.has_active_download && root.is_downloading;
        out property <int> total_items: root.sample_downloads.length + (root.has_active_download ? 1 : 0);

        in-out property <[TableItem]> sample_downloads: [];

        callback start_download();
        callback pause_download();
        callback resume_download();
        callback cancel_download();
        callback browse_folder();
        callback open_file();

        VerticalLayout {
            padding: 0px;
            spacing: 0px;

            // 1. Menu Bar
            Rectangle {
                height: 25px;
                background: #18181b;

                HorizontalLayout {
                    padding-left: 8px;
                    padding-right: 8px;
                    spacing: 2px;
                    alignment: start;

                    Rectangle {
                        width: 48px;
                        background: menu_tasks.has-hover ? #27272a : transparent;
                        menu_tasks := TouchArea { mouse-cursor: pointer; clicked => { root.show_add_dialog = true; } }
                        Text { text: "Tasks"; font-size: 11px; color: #d4d4d8; vertical-alignment: center; horizontal-alignment: center; }
                    }

                    Rectangle {
                        width: 40px;
                        background: menu_file.has-hover ? #27272a : transparent;
                        menu_file := TouchArea {
                            mouse-cursor: pointer;
                            clicked => {
                                if root.has_active_download && root.is_completed { root.open_file(); }
                            }
                        }
                        Text { text: "File"; font-size: 11px; color: #d4d4d8; vertical-alignment: center; horizontal-alignment: center; }
                    }

                    Rectangle {
                        width: 72px;
                        background: menu_dl.has-hover ? #27272a : transparent;
                        menu_dl := TouchArea {
                            mouse-cursor: pointer;
                            clicked => {
                                if root.can_stop { root.pause_download(); }
                                else if root.can_resume { root.resume_download(); }
                            }
                        }
                        Text { text: "Downloads"; font-size: 11px; color: #d4d4d8; vertical-alignment: center; horizontal-alignment: center; }
                    }

                    Rectangle {
                        width: 44px;
                        accessible-role: button;
                        accessible-label: "View (unavailable)";
                        accessible-enabled: false;
                        Text { text: "View"; font-size: 11px; color: #71717a; vertical-alignment: center; horizontal-alignment: center; }
                    }

                    Rectangle {
                        width: 44px;
                        accessible-role: button;
                        accessible-label: "Help (unavailable)";
                        accessible-enabled: false;
                        Text { text: "Help"; font-size: 11px; color: #71717a; vertical-alignment: center; horizontal-alignment: center; }
                    }
                }
            }

            // Divider
            Rectangle { height: 1px; background: #27272a; }

            // 2. Minimal Toolbar (Without Tell Friend)
            Rectangle {
                height: 52px;
                background: #1f1f23;

                HorizontalLayout {
                    padding-left: 8px;
                    padding-right: 8px;
                    padding-top: 2px;
                    padding-bottom: 2px;
                    spacing: 2px;
                    alignment: start;

                    IdmToolButton {
                        text: "Add URL";
                        clicked => { root.show_add_dialog = true; }
                        IconAdd {}
                    }

                    IdmToolButton {
                        text: root.is_paused && !root.is_resumable ? "Restart" : "Resume";
                        enabled: root.can_resume;
                        clicked => { root.resume_download(); }
                        IconResume {}
                    }

                    IdmToolButton {
                        text: "Stop";
                        enabled: root.can_stop;
                        clicked => { root.pause_download(); }
                        IconStop {}
                    }

                    IdmToolButton {
                        text: "Stop All";
                        enabled: root.can_stop_all;
                        // ponytail: the engine has one session, so Stop All shares Stop's safe pause.
                        clicked => { root.pause_download(); }
                        IconStopAll {}
                    }

                    Rectangle { width: 1px; height: 32px; background: #27272a; }

                    IdmToolButton {
                        text: "Delete";
                        enabled: root.active_row_visible && root.selected_row == 0;
                        clicked => { root.cancel_download(); }
                        IconDelete {}
                    }

                    IdmToolButton {
                        text: "Delete C...";
                        enabled: false;
                        IconDeleteCompleted {}
                    }

                    Rectangle { width: 1px; height: 32px; background: #27272a; }

                    IdmToolButton {
                        text: "Options";
                        clicked => { root.show_add_dialog = true; }
                        IconOptions {}
                    }

                }
            }

            // Divider
            Rectangle { height: 1px; background: #27272a; }

            // 3. Main Split Area: Categories on LEFT, Table on RIGHT
            HorizontalLayout {
                spacing: 0px;

                // Left: Categories Tree Panel
                Rectangle {
                    width: 200px;
                    background: #18181b;

                    VerticalLayout {
                        padding: 0px;
                        spacing: 0px;

                        // Categories Header
                        Rectangle {
                            height: 25px;
                            background: #202024;

                            HorizontalLayout {
                                padding-left: 10px;
                                padding-right: 10px;
                                alignment: space-between;

                                Text {
                                    text: "Categories";
                                    font-size: 11px;
                                    font-weight: 600;
                                    color: #a1a1aa;
                                    vertical-alignment: center;
                                }
                            }
                        }

                        // Header Divider
                        Rectangle { height: 1px; background: #27272a; }

                        // Tree Items
                        VerticalLayout {
                            padding: 6px;
                            spacing: 0px;

                            // Node: All Downloads
                            CategoryRow {
                                text: "All Downloads";
                                selected: root.selected_category == 0;
                                expandable: true;
                                expanded: root.all_downloads_expanded;
                                clicked => { root.selected_category = 0; }
                                toggle => {
                                    root.all_downloads_expanded = !root.all_downloads_expanded;
                                    if !root.all_downloads_expanded && root.selected_category >= 1 && root.selected_category <= 5 {
                                        root.selected_category = 0;
                                    }
                                }
                                IconFolder {}
                            }

                            // Subcategories of All Downloads (Tree branches)
                            if root.all_downloads_expanded: VerticalLayout {
                                spacing: 0px;
                                padding: 0px;

                                CategoryRow {
                                    text: "Compressed";
                                    child: true;
                                    selected: root.selected_category == 1;
                                    clicked => { root.selected_category = 1; }
                                    IconArchive {}
                                }

                                CategoryRow {
                                    text: "Documents";
                                    child: true;
                                    selected: root.selected_category == 2;
                                    clicked => { root.selected_category = 2; }
                                    IconDoc {}
                                }

                                CategoryRow {
                                    text: "Music";
                                    child: true;
                                    selected: root.selected_category == 3;
                                    clicked => { root.selected_category = 3; }
                                    IconAudio {}
                                }

                                CategoryRow {
                                    text: "Programs";
                                    child: true;
                                    selected: root.selected_category == 4;
                                    clicked => { root.selected_category = 4; }
                                    IconProgram {}
                                }

                                CategoryRow {
                                    text: "Video";
                                    child: true;
                                    last-child: true;
                                    selected: root.selected_category == 5;
                                    clicked => { root.selected_category = 5; }
                                    IconMedia {}
                                }
                            }

                            Rectangle {
                                height: 6px;
                                Rectangle { x: 24px; y: 3px; width: parent.width - 32px; height: 1px; background: #2e2e34; }
                            }

                            CategoryRow {
                                text: "Unfinished";
                                selected: root.selected_category == 6;
                                clicked => { root.selected_category = 6; }
                                IconUnfinished {}
                            }

                            CategoryRow {
                                text: "Finished";
                                selected: root.selected_category == 7;
                                clicked => { root.selected_category = 7; }
                                IconCheck {}
                            }

                            Rectangle {
                                height: 6px;
                                Rectangle { x: 24px; y: 3px; width: parent.width - 32px; height: 1px; background: #2e2e34; }
                            }

                            CategoryRow {
                                text: "Grabber projects";
                                enabled: false;
                                selected: root.selected_category == 8;
                                clicked => { root.selected_category = 8; }
                                IconGrabber {}
                            }

                            CategoryRow {
                                text: "Queues";
                                enabled: false;
                                selected: root.selected_category == 9;
                                clicked => { root.selected_category = 9; }
                                IconQueue {}
                            }
                        }

                        Rectangle {}
                    }
                }

                // Vertical Divider line between Categories and Table
                Rectangle { width: 1px; background: #27272a; }

                // Right: Downloads Table Panel (Strictly Left-Aligned)
                Rectangle {
                    background: #141416;

                    VerticalLayout {
                        padding: 0px;
                        spacing: 0px;

                        // Table Header Row
                        Rectangle {
                            height: 25px;
                            background: #1f1f23;

                            HorizontalLayout {
                                padding-left: 8px;
                                padding-right: 8px;
                                spacing: 0px;

                                // Col: File Name
                                HorizontalLayout {
                                    horizontal-stretch: 3;
                                    Text { text: "File Name"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                    Rectangle { width: 1px; background: #27272a; }
                                }
                                // Col: Size
                                HorizontalLayout {
                                    width: 80px; padding-left: 6px;
                                    Text { text: "Size"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                    Rectangle { width: 1px; background: #27272a; }
                                }
                                // Col: Status
                                HorizontalLayout {
                                    width: 95px; padding-left: 6px;
                                    Text { text: "Status"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                    Rectangle { width: 1px; background: #27272a; }
                                }
                                // Col: Time Left
                                HorizontalLayout {
                                    width: 80px; padding-left: 6px;
                                    Text { text: "Time left"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                    Rectangle { width: 1px; background: #27272a; }
                                }
                                // Col: Transfer Rate
                                HorizontalLayout {
                                    width: 90px; padding-left: 6px;
                                    Text { text: "Transfer rate"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                    Rectangle { width: 1px; background: #27272a; }
                                }
                                // Col: Last Try Date
                                HorizontalLayout {
                                    width: 90px; padding-left: 6px;
                                    Text { text: "Last Try Date"; font-size: 11px; font-weight: 600; color: #a1a1aa; vertical-alignment: center; }
                                }
                            }
                        }

                        // Header bottom divider
                        Rectangle { height: 1px; background: #27272a; }

                        // Table Rows (Strictly Left-Aligned)
                        VerticalLayout {
                            padding: 0px;
                            spacing: 0px;

                            // Active Download Row
                            if root.active_row_visible: Rectangle {
                                height: 25px;
                                background: root.selected_row == 0 ? #273248 : (act_t.has-hover ? #202024 : #18181b);
                                border-width: row_focus.has-focus ? 1px : 0px;
                                border-color: #8ba9d6;
                                forward-focus: row_focus;
                                accessible-role: button;
                                accessible-label: root.active_filename + ", " + root.active_status;
                                accessible-action-default => { root.selected_row = 0; }

                                DownloadRowColumns {
                                    filename: root.active_filename != "" ? root.active_filename : "New Download";
                                    file-type: root.active_file_type;
                                    size-text: root.active_size;
                                    status-text: root.active_status;
                                    time-left-text: root.active_time_left;
                                    transfer-rate-text: root.active_transfer_rate;
                                    last-try-text: root.active_last_try;
                                    is-active: true;
                                    status-color: root.active_status_color;
                                }
                                Rectangle { y: parent.height - 1px; height: 1px; background: #27272a; }
                                row_focus := FocusScope {
                                    key-pressed(event) => {
                                        if event.text == Key.Space || event.text == Key.Return {
                                            root.selected_row = 0;
                                            return accept;
                                        }
                                        return reject;
                                    }
                                }
                                act_t := TouchArea {
                                    mouse-cursor: pointer;
                                    clicked => { row_focus.focus(); root.selected_row = 0; }
                                    double-clicked => { if root.is_completed { root.open_file(); } }
                                }
                            }

                            // Sample Rows
                            for item[idx] in root.sample_downloads: Rectangle {
                                property <bool> is_visible: root.category_matches(
                                    root.selected_category, item.file_type, true);

                                height: self.is_visible ? 25px : 0px;
                                background: root.selected_row == item.id ? #273248 : (s_touch.has-hover ? #202024 : (Math.mod(idx, 2) == 0 ? #141416 : #18181b));

                                s_touch := TouchArea {
                                    enabled: parent.is_visible;
                                    mouse-cursor: pointer;
                                    clicked => { root.selected_row = item.id; }
                                }

                                if self.is_visible: DownloadRowColumns {
                                    filename: item.filename;
                                    file-type: item.file_type;
                                    size-text: item.size_text;
                                    status-text: item.status_text;
                                    time-left-text: item.time_left_text;
                                    transfer-rate-text: item.transfer_rate_text;
                                    last-try-text: item.last_try_text;
                                }
                                if self.is_visible: Rectangle { y: parent.height - 1px; height: 1px; background: #27272a; }
                            }
                        }

                        Rectangle {
                            if root.total_items == 0 || (root.selected_category == 6 && !root.active_row_visible): VerticalLayout {
                                padding: 24px;
                                spacing: 6px;
                                alignment: center;
                                Text {
                                    text: root.selected_category == 6 ? "No unfinished downloads" : "No downloads";
                                    color: #d4d4d8;
                                    font-size: 13px;
                                    horizontal-alignment: center;
                                }
                                Text {
                                    text: root.selected_category == 6 ? "Active, stopped, and failed downloads appear here." :
                                        "Choose Add URL to start a download.";
                                    color: #a1a1aa;
                                    font-size: 11px;
                                    horizontal-alignment: center;
                                    wrap: word-wrap;
                                }
                            }
                        }
                        if root.action_error_message != "" || (root.active_row_visible &&
                            (root.active_error_message != "" || root.is_paused)): Rectangle {
                            min-height: 48px;
                            vertical-stretch: 0;
                            background: #202024;
                            VerticalLayout {
                                padding: 12px;
                                Text {
                                    text: root.action_error_message != "" ? root.action_error_message :
                                        (root.active_error_message != "" ? root.active_error_message :
                                        (root.is_resumable ? "Stopped. Select the download and choose Resume to continue." :
                                        "Stopped. Resume is unavailable for this download; Restart downloads the file from the beginning."));
                                    color: #d4d4d8;
                                    font-size: 11px;
                                    wrap: word-wrap;
                                    vertical-alignment: center;
                                }
                            }
                        }
                    }
                }
            }

            // Divider
            Rectangle { height: 1px; background: #27272a; }

            // 4. Status Bar
            Rectangle {
                height: 22px;
                background: #18181b;

                HorizontalLayout {
                    padding-left: 10px;
                    padding-right: 10px;
                    spacing: 12px;

                    Text {
                        text: (root.selected_category == 6 ? (root.active_row_visible ? "1 unfinished item" : "0 unfinished items") :
                            root.total_items + (root.total_items == 1 ? " total item" : " total items")) + " | " +
                            (root.is_downloading ? "1 active download" : "Idle");
                        font-size: 11px;
                        color: #71717a;
                        vertical-alignment: center;
                    }

                    Rectangle {}

                    Text {
                        text: "Transfer: " + (root.is_downloading ? root.active_transfer_rate : "0 KB/s") + " | Streams: " + Math.round(root.streams_count);
                        font-size: 11px;
                        color: #71717a;
                        vertical-alignment: center;
                    }
                }
            }
        }

        // 5. Minimal Add URL Dialog
        if root.show_add_dialog: Rectangle {
            background: #000000bb;

            TouchArea {}

            Rectangle {
                x: (parent.width - self.width) / 2;
                y: (parent.height - self.height) / 2;
                width: 460px;
                height: 248px;
                background: #1f1f23;

                VerticalLayout {
                    padding: 20px;
                    spacing: 12px;

                    Text {
                        height: 24px;
                        text: "Add New Download";
                        font-size: 15px;
                        font-weight: 600;
                        color: #f4f4f5;
                    }

                    HorizontalLayout {
                        spacing: 8px;
                        Text { text: "URL:"; width: 55px; font-size: 11px; color: #a1a1aa; vertical-alignment: center; }
                        DialogInput {
                            text <=> root.url_text;
                            placeholder-text: "https://example.com/file.zip";
                            label: "URL";
                        }
                    }

                    HorizontalLayout {
                        spacing: 8px;
                        Text { text: "Save to:"; width: 55px; font-size: 11px; color: #a1a1aa; vertical-alignment: center; }
                        DialogInput { text <=> root.dest_dir_text; label: "Save to"; }
                        PointerButton {
                            text: "Browse";
                            clicked => { root.browse_folder(); }
                        }
                    }

                    HorizontalLayout {
                        spacing: 8px;
                        Text { text: "Streams:"; width: 55px; font-size: 11px; color: #a1a1aa; vertical-alignment: center; }
                        DialogSlider {
                            value <=> root.streams_count;
                        }
                        Text {
                            text: Math.round(root.streams_count) + " threads";
                            width: 65px; font-size: 11px; color: #a1a1aa; vertical-alignment: center;
                        }
                    }

                    HorizontalLayout {
                        alignment: end;
                        spacing: 8px;

                        PointerButton {
                            text: "Cancel";
                            clicked => { root.show_add_dialog = false; }
                        }

                        PointerButton {
                            text: "Download";
                            primary: true;
                            enabled: root.url_text != "";
                            clicked => {
                                root.show_add_dialog = false;
                                root.start_download();
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::platform::default_download_directory;
    use super::*;

    #[test]
    fn controls_and_filters_support_pointer_and_keyboard() -> Result<(), Box<dyn std::error::Error>>
    {
        use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
        use slint::platform::{Platform, PointerEventButton, WindowAdapter, WindowEvent};
        use std::rc::Rc;

        struct TestPlatform(Rc<MinimalSoftwareWindow>);
        impl Platform for TestPlatform {
            fn create_window_adapter(&self) -> Result<Rc<dyn WindowAdapter>, slint::PlatformError> {
                Ok(self.0.clone())
            }
        }

        let window = MinimalSoftwareWindow::new(RepaintBufferType::ReusedBuffer);
        slint::platform::set_platform(Box::new(TestPlatform(window.clone())))?;
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
            ui.on_cancel_download(|| panic!("Stop All must not cancel the session"));

            use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
            let mut snapshot = DownloadSnapshot {
                filename: "archive.zip".into(),
                status: DownloadStatus::Downloading,
                resumable: true,
                ..Default::default()
            };
            super::super::update_window_state(&ui, &snapshot);
            assert_eq!(ui.get_total_items(), 8);
            ui.set_selected_category(6);
            ui.set_selected_row(1);
            render();
            assert!(ui.get_active_row_visible());
            assert!(
                !ui.get_can_stop(),
                "Stop only targets the selected download"
            );
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

            snapshot.status = DownloadStatus::Paused;
            super::super::update_window_state(&ui, &snapshot);
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
            super::super::update_window_state(&ui, &snapshot);
            render();
            click(102.0, 50.0);
            assert_eq!(resumed.get(), 2, "Non-range downloads can restart");
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
                super::super::update_window_state(&ui, &snapshot);
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
            super::super::update_window_state(&ui, &snapshot);
            for category in 0..10 {
                ui.set_selected_category(category);
                assert_eq!(ui.get_active_row_visible(), matches!(category, 0 | 1 | 7));
            }
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            assert!(super::super::send_action(&ui, &tx, DownloadAction::Pause));
            assert!(!super::super::send_action(&ui, &tx, DownloadAction::Resume));
            assert!(ui.get_action_error_message().contains("busy"));
            drop(rx);
            assert!(!super::super::send_action(&ui, &tx, DownloadAction::Pause));
            assert!(ui.get_action_error_message().contains("unavailable"));
            ui.set_action_error_message("".into());
            super::super::update_window_state(&ui, &DownloadSnapshot::default());
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
}
