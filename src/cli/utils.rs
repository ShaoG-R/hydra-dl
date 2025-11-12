use super::{CliError, Result};
use std::path::PathBuf;

/// 从 URL 提取文件名
///
/// # Example
/// ```
/// # use hydra_dl::cli::utils::extract_filename_from_url;
/// let filename = extract_filename_from_url("https://example.com/path/file.zip").unwrap();
/// assert_eq!(filename, "file.zip");
/// ```
pub fn extract_filename_from_url(url: &str) -> Result<String> {
    // 解析 URL
    let parsed_url = url::Url::parse(url)?;

    // 获取路径段
    let mut path_segments = parsed_url
        .path_segments()
        .ok_or_else(|| CliError::Path("无法提取 URL 路径".to_string()))?;

    // 获取最后一个段作为文件名
    let filename = path_segments
        .next_back()
        .ok_or_else(|| CliError::Path("URL 中没有文件名".to_string()))?;

    if filename.is_empty() {
        return Err(CliError::Path(
            "无法从 URL 提取文件名，请使用 -o 参数指定输出路径".to_string(),
        ));
    }

    // URL 解码
    let decoded = urlencoding::decode(filename)
        .map_err(|e| CliError::UrlDecode(e.to_string()))?
        .to_string();

    Ok(decoded)
}

/// 验证和准备输出路径
///
/// 检查路径是否已存在，如果父目录不存在则创建
pub fn validate_output_path(path: &str) -> Result<PathBuf> {
    let path_buf = PathBuf::from(path);

    // 如果文件已存在，警告用户
    if path_buf.exists() {
        eprintln!("警告: 文件 {:?} 已存在，将被覆盖", path_buf);
    }

    // 确保父目录存在
    if let Some(parent) = path_buf.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent).map_err(CliError::Io)?;
    }

    Ok(path_buf)
}

/// 格式化字节数为人类可读格式
///
/// # Example
/// ```
/// # use hydra_dl::cli::utils::format_bytes;
/// assert_eq!(format_bytes(1024), "1.00 KB");
/// assert_eq!(format_bytes(1048576), "1.00 MB");
/// assert_eq!(format_bytes(1073741824), "1.00 GB");
/// ```
pub fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let bytes_f = bytes as f64;

    if bytes_f >= GB {
        format!("{:.2} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.2} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.2} KB", bytes_f / KB)
    } else {
        format!("{} B", bytes)
    }
}

/// 格式化时长为人类可读格式
///
/// # Example
/// ```
/// # use hydra_dl::cli::utils::format_duration;
/// assert_eq!(format_duration(90.5), "1m 30s");
/// assert_eq!(format_duration(3661.0), "1h 1m");
/// ```
pub fn format_duration(seconds: f64) -> String {
    let total_secs = seconds as u64;

    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if hours > 0 {
        if minutes > 0 {
            format!("{}h {}m", hours, minutes)
        } else {
            format!("{}h", hours)
        }
    } else if minutes > 0 {
        if secs > 0 {
            format!("{}m {}s", minutes, secs)
        } else {
            format!("{}m", minutes)
        }
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_from_url() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file.zip").unwrap(),
            "file.zip"
        );
        assert_eq!(
            extract_filename_from_url("https://example.com/path/to/file.tar.gz").unwrap(),
            "file.tar.gz"
        );
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1536), "1.50 KB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30.0), "30s");
        assert_eq!(format_duration(90.0), "1m 30s");
        assert_eq!(format_duration(60.0), "1m");
        assert_eq!(format_duration(3600.0), "1h");
        assert_eq!(format_duration(3661.0), "1h 1m");
        assert_eq!(format_duration(3720.0), "1h 2m");
    }
}
