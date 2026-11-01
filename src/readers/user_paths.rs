//! 通过目标平台的用户目录 API 取得默认数据根锚点。

use std::path::PathBuf;

/// 返回 OS API 解析的当前用户主目录，不直接读取 Windows/macOS 的 HOME/USERPROFILE。
pub fn current_user_home() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let path = PathBuf::from(objc2_foundation::NSHomeDirectory().to_string());
        path.is_absolute().then_some(path)
    }
    #[cfg(target_os = "windows")]
    {
        windows::Storage::UserDataPaths::GetDefault()
            .ok()
            .and_then(|paths| paths.Profile().ok())
            .map(|profile| PathBuf::from(profile.to_string()))
            .filter(|path| path.is_absolute())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
    }
}

/// 返回当前用户的第三方应用数据根目录，用于定位其他桌面应用写在本机的数据。
///
/// 该目录是宿主约定而非本产品业务规则：macOS 为 `~/Library/Application Support`，
/// Windows 为 Roaming AppData，其余平台按 XDG 约定回退到 `~/.config`。只返回绝对
/// 路径，解析失败时返回 `None` 由调用方按“可选目录缺失”处理。
pub fn current_user_app_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        current_user_home().map(|home| home.join("Library").join("Application Support"))
    }
    #[cfg(target_os = "windows")]
    {
        windows::Storage::UserDataPaths::GetDefault()
            .ok()
            .and_then(|paths| paths.RoamingAppData().ok())
            .map(|roaming| PathBuf::from(roaming.to_string()))
            .filter(|path| path.is_absolute())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .or_else(|| current_user_home().map(|home| home.join(".config")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证读取到的当前用户主目录总是绝对路径。
    #[test]
    fn current_platform_user_home_is_absolute_when_available() {
        let home = current_user_home().expect("current platform user home is available");
        assert!(home.is_absolute());
    }

    /// 验证第三方应用数据根目录可解析且始终是绝对路径。
    #[test]
    fn current_platform_user_app_data_dir_is_absolute_when_available() {
        let app_data = current_user_app_data_dir().expect("current platform app data is available");
        assert!(app_data.is_absolute());
    }
}
