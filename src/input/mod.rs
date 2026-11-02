//! 在只读边界内打开来源并保留同一文件句柄，增量状态由客户端自行维护。
use crate::readers::claude::path_rules::validate_transcript_path;
use crate::readers::discovery::{
    metadata_is_link_like, validate_local_plain_directory, walk_ancestors_for_link_component,
};
use crate::readers::file_source::{
    ValidatedRolloutFile, file_identity, is_rollout_jsonl, map_source_io_error, modified_epoch_ms,
    same_opened_file,
};
use crate::readers::grok::path_rules::{
    grok_project_seed, grok_session_seed, is_grok_updates_path,
};
use crate::{LocalError, LocalErrorKind, path_key, stable_id};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
/// 消费方明确选择的来源位置和稳定根标识；不读取产品设置或登记库。
#[derive(Debug, Clone)]
pub struct SourceFileRequest {
    /// 原生层授权的来源根；仅在文件校验中使用。
    pub root_path: PathBuf,
    /// 当前准备读取的文件路径，不应发送到界面或网络。
    pub file_path: PathBuf,
    /// 由消费方维护的非敏感稳定根标识，须满足技术 ID 字符边界。
    pub root_id: String,
}

/// 一次来源打开时的观察信息；不包含数据库 generation 或保留策略。
/// 项目和线程种子仅用于本机身份派生，不应序列化或记录日志。
#[derive(Clone)]
pub struct UsageFileMetadata {
    /// 从原生路径组件确定是否为归档来源，不从编码标签反推。
    pub archived: bool,
    /// 来源稳定标识。
    pub source_id: String,
    /// 所属来源根的稳定标识。
    pub root_id: String,
    /// 根内相对标签。
    pub relative_label: String,
    /// 既有首64字节指纹，不是操作系统物理文件身份。
    pub fingerprint: String,
    /// 同一打开句柄观测到的字节上界。
    pub observed_size: u64,
    /// 同一次打开的修改时间毫秒值。
    pub modified_at_epoch_ms: i64,
    /// 本机派生项目身份的原料，不进入对外报告。
    pub project_seed: Option<String>,
    /// 本机派生线程身份的原料，不进入对外报告。
    pub thread_seed: String,
}

impl std::fmt::Debug for UsageFileMetadata {
    /// 隐去原始路径种子，调试只显示内容无关身份与资源观察。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageFileMetadata")
            .field("source_id", &self.source_id)
            .field("observed_size", &self.observed_size)
            .field("modified_at_epoch_ms", &self.modified_at_epoch_ms)
            .finish_non_exhaustive()
    }
}

/// 已完成路径、链接与同文件检查的只读来源；后续解析持续使用该句柄。
#[derive(Debug)]
pub struct OpenedUsageFile {
    file: File,
    metadata: UsageFileMetadata,
}

impl OpenedUsageFile {
    /// 借用来源观察，不重新打开路径。
    pub fn metadata(&self) -> &UsageFileMetadata {
        &self.metadata
    }
    /// 交付同一只读句柄与观察值，供消费方接入自己的增量决策。
    pub fn into_parts(self) -> (File, UsageFileMetadata) {
        (self.file, self.metadata)
    }
}

/// 归一化已批准的平台根别名；先拒绝父目录，再验证本地根，最后比较原生路径组件。
fn normalize_request(mut request: SourceFileRequest) -> Result<SourceFileRequest, LocalError> {
    if !crate::is_safe_usage_filter_id(&request.root_id) {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "source root identifier is invalid",
        ));
    }
    if !request.root_path.is_absolute()
        || !request.file_path.is_absolute()
        || request
            .root_path
            .components()
            .chain(request.file_path.components())
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "source paths must be absolute without parent components",
        ));
    }
    #[cfg(windows)]
    reject_ambiguous_windows_components(&request.file_path)?;
    let root = canonical_source_root(&request.root_path)?;
    let comparable_root = comparable_path(&root).map_err(map_source_io_error)?;
    let comparable_file = comparable_path(&request.file_path).map_err(map_source_io_error)?;
    let relative = relative_with_platform_alias(&comparable_root, &comparable_file)?;
    request.file_path = root.join(relative);
    request.root_path = root;
    Ok(request)
}

/// 验证本地普通目录并归一化平台路径别名，供宿主派生一致的数据根身份。
///
/// # Errors
/// 非绝对路径、父目录跳转、链接、网络卷或无法读取的目录返回脱敏来源错误。
pub fn canonical_source_root(path: &Path) -> Result<PathBuf, LocalError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "source root must be absolute without parent components",
        ));
    }
    validate_local_plain_directory(path).map_err(map_source_io_error)?;
    fs::canonicalize(path).map_err(map_source_io_error)
}

/// 普通 Win32 路径不能带会被 verbatim 根改变含义的尾随点或空格组件。
#[cfg(windows)]
fn reject_ambiguous_windows_components(path: &Path) -> Result<(), LocalError> {
    use std::os::windows::ffi::OsStrExt;
    use std::path::{Component, Prefix};
    if let Some(Component::Prefix(prefix)) = path.components().next()
        && matches!(
            prefix.kind(),
            Prefix::VerbatimDisk(_) | Prefix::VerbatimUNC(_, _) | Prefix::Verbatim(_)
        )
    {
        return Ok(());
    }
    for part in path.components() {
        if let Component::Normal(value) = part
            && matches!(value.encode_wide().last(), Some(32 | 46))
        {
            return Err(LocalError::new(
                LocalErrorKind::InvalidPath,
                "source has ambiguous Win32 components",
            ));
        }
    }
    Ok(())
}

/// 比较原生根边界；Windows 根别名必须通过操作系统目录身份验证，不能盲目折叠大小写。
fn relative_with_platform_alias(root: &Path, file: &Path) -> Result<PathBuf, LocalError> {
    if let Ok(relative) = crate::readers::file_source::source_relative_path(root, file) {
        return Ok(relative.to_path_buf());
    }
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        let drive = |path: &Path| match path.components().next() {
            Some(Component::Prefix(prefix)) => match prefix.kind() {
                Prefix::Disk(value) | Prefix::VerbatimDisk(value) => {
                    Some(value.to_ascii_uppercase())
                }
                _ => None,
            },
            _ => None,
        };
        // 只允许同一本地磁盘上的候选根；不向 UNC 或其他磁盘探测。
        if drive(root).is_some() && drive(root) == drive(file) {
            let candidate: PathBuf = file.components().take(root.components().count()).collect();
            let relative = crate::readers::file_source::source_relative_path(&candidate, file)?;
            validate_local_plain_directory(&candidate).map_err(map_source_io_error)?;
            if same_file::is_same_file(root, &candidate).map_err(map_source_io_error)? {
                return Ok(relative.to_path_buf());
            }
        }
    }
    Err(LocalError::new(
        LocalErrorKind::InvalidPath,
        "source escaped its approved root",
    ))
}

/// 仅折叠平台自身允许的根别名和磁盘前缀；不解析用户创建的来源链接。
fn comparable_path(path: &Path) -> std::io::Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        for alias in ["/var", "/tmp", "/etc"] {
            if let Ok(relative) = path.strip_prefix(alias) {
                return fs::canonicalize(alias).map(|root| root.join(relative));
            }
        }
    }
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        let mut components = path.components();
        if let Some(Component::Prefix(prefix)) = components.next()
            && let Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) = prefix.kind()
        {
            let mut normalized =
                PathBuf::from(format!("{}:\\", char::from(drive.to_ascii_uppercase())));
            for component in components {
                if !matches!(component, Component::RootDir) {
                    normalized.push(component.as_os_str());
                }
            }
            return Ok(normalized);
        }
    }
    Ok(path.to_path_buf())
}
/// 在 blocking 岛内完成相对路径、指纹与打开句柄移交。
pub fn open_rollout_source(
    mut source: ValidatedRolloutFile,
    request: SourceFileRequest,
) -> Result<OpenedUsageFile, LocalError> {
    let request = normalize_request(request)?;
    let archived = validate_rollout_request(&request)?;
    let SourceFileRequest {
        root_path,
        file_path,
        root_id,
    } = request;
    let validated = normalize_request(SourceFileRequest {
        root_path: root_path.clone(),
        file_path: source.path().to_path_buf(),
        root_id: root_id.clone(),
    })?;
    if validated.file_path != file_path {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "validated source path does not match request",
        ));
    }
    let observed_size = source.metadata().len();
    let modified_at_epoch_ms = modified_epoch_ms(source.metadata());
    let relative = file_path.strip_prefix(&root_path).map_err(|_| {
        LocalError::new(
            LocalErrorKind::InvalidPath,
            "rollout source escaped its root",
        )
    })?;
    let relative_label = path_key(relative);
    let source_id = stable_id("source", &format!("{root_id}\u{0}{relative_label}"));
    let file_identity = file_identity(source.file_mut())?;
    Ok(OpenedUsageFile {
        file: source.into_file(),
        metadata: UsageFileMetadata {
            archived,
            thread_seed: relative_label.clone(),
            project_seed: None,
            source_id,
            root_id,
            relative_label,
            fingerprint: file_identity,
            observed_size,
            modified_at_epoch_ms,
        },
    })
}

/// 在 blocking 岛内打开、校验并计算 Claude transcript 身份。
pub fn open_claude_source(request: SourceFileRequest) -> Result<OpenedUsageFile, LocalError> {
    let request = normalize_request(request)?;
    validate_transcript_path(&request.root_path, &request.file_path)?;
    let SourceFileRequest {
        root_path,
        file_path,
        root_id,
    } = request;
    let mut file = File::open(&file_path).map_err(map_source_io_error)?;
    let metadata = file.metadata().map_err(map_source_io_error)?;
    let current_metadata = fs::symlink_metadata(&file_path).map_err(map_source_io_error)?;
    if !metadata.is_file()
        || !current_metadata.is_file()
        || metadata_is_link_like(&current_metadata)
        || walk_ancestors_for_link_component(&file_path).map_err(map_source_io_error)?
        || !same_opened_file(&file, &file_path, &metadata, &current_metadata)
            .map_err(map_source_io_error)?
    {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "Claude transcript source changed during validation",
        ));
    }
    let normalized_path = fs::canonicalize(&file_path).map_err(map_source_io_error)?;
    validate_transcript_path(&root_path, &normalized_path)?;
    let observed_size = metadata.len();
    let modified_at_epoch_ms = modified_epoch_ms(&metadata);
    let relative = normalized_path.strip_prefix(&root_path).map_err(|_| {
        LocalError::new(
            LocalErrorKind::InvalidPath,
            "Claude transcript escaped its root",
        )
    })?;
    let relative_label = path_key(relative);
    let source_id = stable_id("claude-source", &format!("{root_id}\u{0}{relative_label}"));
    let identity = file_identity(&mut file)?;
    let project_seed = relative
        .components()
        .nth(1)
        .map(|component| path_key(Path::new(component.as_os_str())));
    Ok(OpenedUsageFile {
        file,
        metadata: UsageFileMetadata {
            archived: false,
            source_id,
            root_id,
            thread_seed: relative_label.clone(),
            relative_label,
            fingerprint: identity,
            observed_size,
            modified_at_epoch_ms,
            project_seed,
        },
    })
}

/// 打开 Grok 来源并观察文件身份，不读取或保存客户端解析检查点。
pub fn open_grok_source(request: SourceFileRequest) -> Result<OpenedUsageFile, LocalError> {
    let request = normalize_request(request)?;
    let relative =
        crate::readers::file_source::source_relative_path(&request.root_path, &request.file_path)?;
    if !is_grok_updates_path(relative) {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "source is outside the approved Grok layout",
        ));
    }
    let SourceFileRequest {
        root_path,
        file_path,
        root_id,
    } = request;
    let mut file = File::open(&file_path).map_err(map_source_io_error)?;
    let metadata = file.metadata().map_err(map_source_io_error)?;
    let current_metadata = fs::symlink_metadata(&file_path).map_err(map_source_io_error)?;
    if !metadata.is_file()
        || !current_metadata.is_file()
        || metadata_is_link_like(&current_metadata)
        || walk_ancestors_for_link_component(&file_path).map_err(map_source_io_error)?
        || !same_opened_file(&file, &file_path, &metadata, &current_metadata)
            .map_err(map_source_io_error)?
    {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "Grok session source changed during validation",
        ));
    }
    let normalized_path = fs::canonicalize(&file_path).map_err(map_source_io_error)?;
    let relative = normalized_path.strip_prefix(&root_path).map_err(|_| {
        LocalError::new(LocalErrorKind::InvalidPath, "Grok updates escaped its root")
    })?;
    if !is_grok_updates_path(relative) {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "Grok updates path is outside the approved sessions layout",
        ));
    }
    let relative_label = path_key(relative);
    let thread_seed = grok_session_seed(relative).unwrap_or_else(|| relative_label.clone());
    let source_id = stable_id("grok-source", &format!("{root_id}\u{0}{relative_label}"));
    let identity = file_identity(&mut file)?;
    Ok(OpenedUsageFile {
        file,
        metadata: UsageFileMetadata {
            archived: false,
            source_id,
            root_id,
            relative_label,
            fingerprint: identity,
            observed_size: metadata.len(),
            modified_at_epoch_ms: modified_epoch_ms(&metadata),
            project_seed: grok_project_seed(relative),
            thread_seed,
        },
    })
}

mod read;
pub use read::{
    JsonlFormat, USAGE_READ_REPORT_SCHEMA_VERSION, UsageReadOptions, UsageReadReport,
    read_usage_file,
};

/// 在打开 Codex 路径之前拒绝认证文件、根外路径和非会话布局。
fn validate_rollout_request(request: &SourceFileRequest) -> Result<bool, LocalError> {
    let relative =
        crate::readers::file_source::source_relative_path(&request.root_path, &request.file_path)?;
    let area = relative
        .components()
        .next()
        .and_then(|part| part.as_os_str().to_str());
    if !matches!(area, Some("sessions" | "archived_sessions"))
        || !is_rollout_jsonl(&request.file_path)
    {
        return Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "source is outside the approved rollout layout",
        ));
    }
    Ok(area == Some("archived_sessions"))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    /// 非 UTF-8 标签编码不能改变原生归档区域；该路径计算不要求宿主文件系统支持非法 UTF-8 名称。
    #[test]
    fn archived_area_is_derived_before_non_utf8_path_encoding() {
        let root = PathBuf::from("/fixture");
        let file = root
            .join("archived_sessions")
            .join(OsString::from_vec(vec![b'p', 0xff]))
            .join("rollout-a.jsonl");
        let request = SourceFileRequest {
            root_path: root.clone(),
            file_path: file,
            root_id: "root".into(),
        };
        assert!(validate_rollout_request(&request).expect("normal native components"));
        let label = path_key(request.file_path.strip_prefix(&root).expect("relative"));
        assert!(
            !label.starts_with("archived_sessions/"),
            "fixture must exercise encoded path namespace"
        );
    }
}

#[cfg(feature = "storage")]
pub(crate) use read::read_usage_file_observed;
