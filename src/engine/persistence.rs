use super::*;

pub(super) fn persist_snapshot(path: &Path, snapshot: &TaskSnapshot) -> Result<()> {
    ensure_parent_dir(path)?;
    let bytes = bincode::serialize(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(super) fn load_snapshot(path: &Path) -> Result<TaskSnapshot> {
    let bytes = std::fs::read(path)?;
    Ok(bincode::deserialize(&bytes)?)
}


pub(super) fn log_path(task: &DownloadTask) -> PathBuf {
    log_root(task).join(format!("{}.log", task.filename))
}

pub(super) fn metadata_path(task: &DownloadTask) -> PathBuf {
    meta_root(task).join(format!("{}.tur.meta", task.filename))
}

pub(super) fn log_root(task: &DownloadTask) -> PathBuf {
    if let Some(root) = &task.log_root {
        return root.join("tur");
    }
    task.dir.join(".tur").join("logs")
}

pub(super) fn meta_root(task: &DownloadTask) -> PathBuf {
    if let Some(root) = &task.log_root {
        return root.join("tur-meta");
    }
    task.dir.join(".tur").join("meta")
}

pub(super) fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}


pub(super) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
