//! 网络配置模块

use std::time::Duration;

// ==================== 常量 ====================

/// 网络配置常量
pub struct Defaults;

impl Defaults {
    /// 请求超时时间：30 秒
    pub const TIMEOUT_SECS: u64 = 30;
    /// 连接超时时间：10 秒
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;
}

// ==================== 配置结构体 ====================

/// 网络配置
///
/// 控制 HTTP 请求的超时设置
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// HTTP 请求总体超时时间
    pub timeout: Duration,
    /// HTTP 连接超时时间
    pub connect_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(Defaults::TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(Defaults::CONNECT_TIMEOUT_SECS),
        }
    }
}

impl NetworkConfig {
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }
}

// ==================== 构建器 ====================

/// 网络配置构建器
#[derive(Debug, Clone)]
pub struct NetworkConfigBuilder {
    pub(crate) timeout: Duration,
    pub(crate) connect_timeout: Duration,
}

impl NetworkConfigBuilder {
    /// 创建新的网络配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(Defaults::TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(Defaults::CONNECT_TIMEOUT_SECS),
        }
    }

    /// 设置 HTTP 请求总体超时时间
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 设置 HTTP 连接超时时间
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// 构建网络配置
    pub fn build(self) -> NetworkConfig {
        NetworkConfig {
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
        }
    }
}

impl Default for NetworkConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
