/// Input mode for the TUI.
#[derive(PartialEq)]
pub(super) enum InputMode {
    Normal,
    UrlInput,
    DirInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FocusPane {
    TaskList,
    Details,
}
