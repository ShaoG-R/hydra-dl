# 🐉 Hydra-DL

<div align="center">

**高性能 Rust 多线程下载器**

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## ✨ 特性

- 🚀 **极速下载** - 多 worker 并发下载，充分利用带宽
- 🎯 **智能分块** - 根据实时速度自动调整分块大小（2-50 MB）
- 🔄 **断点续传** - 支持 HTTP Range 请求，单文件分段下载
- 📊 **实时监控** - 详细的进度统计、速度/加速度监测及精准 ETA
- 🛡️ **稳定可靠** - 自动重试机制，失败任务智能调度
- 🧵 **渐进式启动** - 分批启动 worker，避免服务器压力
- 💾 **内存友好** - 流式写入，内存占用低
- 🎨 **双重接口** - CLI 工具 + Rust 库，灵活集成

## 📦 安装

### 从源码构建

```bash
git clone https://github.com/ShaoG-R/hydra-dl.git
cd hydra-dl
cargo build --release
```

构建完成后，可执行文件位于 `target/release/hydra-dl`

### 作为库使用

目前处于开发中，请你慎重使用

## 🚀 快速开始

### CLI 使用

#### 基础下载

```bash
# 下载到当前目录
hydra-dl https://example.com/file.zip

# 指定保存目录
hydra-dl https://example.com/file.zip -o ./downloads
```

#### 高级配置

```bash
# 自定义并发数和分块大小
hydra-dl https://example.com/large_file.bin \
  --workers 8 \
  --initial-workers 2 \
  --chunk-size 10 \
  --min-chunk 5 \
  --max-chunk 100

# 静默模式（无进度条）
hydra-dl https://example.com/file.zip --quiet

# 详细模式（显示每个 worker 统计）
hydra-dl https://example.com/file.zip --verbose
```

#### 完整参数列表

```
选项:
  -o, --output <DIRECTORY>     保存目录（默认：当前目录）
  -n, --workers <NUM>          Worker 并发数（默认：4）
      --initial-workers <NUM>  初始 Worker 数量（默认：1）
      --chunk-size <MB>        初始分块大小（默认：5 MB）
      --min-chunk <MB>         最小分块大小（默认：2 MB）
      --max-chunk <MB>         最大分块大小（默认：50 MB）
  -q, --quiet                  静默模式
  -v, --verbose                详细模式
  -h, --help                   显示帮助信息
  -V, --version                显示版本信息
```

### 库使用

#### 简单下载（单线程）

```rust
use hydra_dl::download_file;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    let save_path = download_file("https://example.com/file.zip", ".").await?;
    println!("文件已保存到: {:?}", save_path);
    Ok(())
}
```

#### Range 分段下载（多线程）

```rust
use hydra_dl::{download_ranged, DownloadConfig, DownloadProgress};
use hydra_dl::timer::{TimerWheel, ServiceConfig};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    // 创建配置
    let config = DownloadConfig::builder()
        .worker_count(8)                         // 8 个并发 worker
        .initial_chunk_size(10 * 1024 * 1024)    // 初始 10 MB
        .min_chunk_size(5 * 1024 * 1024)         // 最小 5 MB
        .max_chunk_size(100 * 1024 * 1024)       // 最大 100 MB
        .build()?;
    
    // 创建定时器服务
    let timer = TimerWheel::with_defaults();
    let timer_service = timer.create_service(ServiceConfig::default());
    
    // 开始下载
    let (mut handle, save_path) = download_ranged(
        "https://example.com/large_file.zip",
        PathBuf::from("."),
        config,
        timer_service
    ).await?;

    println!("文件将保存到: {:?}", save_path);

    // 监听进度
    while let Some(progress) = handle.progress_receiver().recv().await {
        match progress {
            DownloadProgress::Started { total_size, worker_count, .. } => {
                println!("开始下载，大小: {:.2} MB，{} 个 workers", 
                    total_size as f64 / 1024.0 / 1024.0, worker_count);
            }
            DownloadProgress::Progress { percentage, avg_speed, worker_stats, .. } => {
                println!("进度: {:.1}%，速度: {:.2} MB/s，{} 个 workers 活跃", 
                    percentage, 
                    avg_speed / 1024.0 / 1024.0,
                    worker_stats.len());
            }
            DownloadProgress::Completed { total_bytes, total_time, .. } => {
                println!("下载完成！{:.2} MB 用时 {:.2}s", 
                    total_bytes as f64 / 1024.0 / 1024.0, total_time);
            }
            DownloadProgress::Error { message } => {
                eprintln!("下载出错: {}", message);
            }
        }
    }

    // 等待下载完成
    handle.wait().await?;
    Ok(())
}
```

#### 使用回调函数简化进度监听

```rust
use hydra_dl::{download_ranged, DownloadConfig, DownloadProgress};
use hydra_dl::timer::{TimerWheel, ServiceConfig};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    let config = DownloadConfig::default();
    let timer = TimerWheel::with_defaults();
    let timer_service = timer.create_service(ServiceConfig::default());
    
    let (handle, save_path) = download_ranged(
        "https://example.com/file.zip",
        PathBuf::from("."),
        config,
        timer_service
    ).await?;

    println!("下载到: {:?}", save_path);

    // 使用回调函数监听进度并等待完成
    handle.wait_with_progress(|progress| {
        if let DownloadProgress::Progress { percentage, avg_speed, .. } = progress {
            println!("进度: {:.1}%，速度: {:.2} MB/s", 
                percentage, avg_speed / 1024.0 / 1024.0);
        }
    }).await?;

    Ok(())
}
```

## 🔧 核心机制

### 动态分块策略

Hydra-DL 会根据实时下载速度自动调整每个 worker 的分块大小：

- **高速网络** → 增大分块（减少请求次数）
- **低速网络** → 减小分块（提高响应性）
- **独立调整** → 每个 worker 独立计算最优分块大小

### 渐进式启动

避免一次性启动所有 worker 对服务器造成压力：

```rust
let config = DownloadConfig::builder()
    .worker_count(12)
    .progressive_worker_ratios(vec![0.25, 0.5, 0.75, 1.0])  // 分4批启动
    .min_speed_threshold(5 * 1024 * 1024)  // 速度阈值 5 MB/s
    .build()?;
```

- **第 1 批**：启动 25% workers（3个）
- **第 2 批**：速度达标后启动到 50%（6个）
- **第 3 批**：继续达标后启动到 75%（9个）
- **第 4 批**：最终启动全部 100%（12个）

### 智能重试机制

失败任务自动重试，支持自定义延迟序列：

```rust
let config = DownloadConfig::builder()
    .max_retry_count(5)
    .retry_delays(vec![
        Duration::from_secs(1),   // 第1次重试：1秒后
        Duration::from_secs(2),   // 第2次重试：2秒后
        Duration::from_secs(5),   // 第3次及以后：5秒后
    ])
    .build()?;
```

## 📊 性能优势

| 特性 | Hydra-DL | 传统下载器 |
|------|----------|-----------|
| 并发下载 | ✅ 多 worker 并行 | ❌ 单线程 |
| 动态分块 | ✅ 自适应调整 | ❌ 固定大小 |
| 渐进式启动 | ✅ 分批启动 | ❌ 一次性启动 |
| 失败重试 | ✅ 智能延迟 | ⚠️ 简单重试 |
| 内存占用 | ✅ 流式写入 | ⚠️ 缓存累积 |
| 进度监控 | ✅ 详细统计 | ⚠️ 简单百分比 |

## 🛠️ 技术栈

- **异步运行时**: Tokio
- **HTTP 客户端**: Reqwest
- **定时器**: Kestrel-Protocol-Timer
- **CLI 工具**: Clap
- **进度条**: Indicatif

## 📝 配置说明

### DownloadConfig 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `worker_count` | 4 | Worker 并发数 |
| `initial_chunk_size` | 5 MB | 初始分块大小 |
| `min_chunk_size` | 2 MB | 最小分块大小 |
| `max_chunk_size` | 50 MB | 最大分块大小 |
| `max_retry_count` | 3 | 最大重试次数 |
| `progressive_worker_ratios` | `[1.0]` | 渐进式启动比例 |
| `min_speed_threshold` | 0 | 速度阈值（字节/秒） |
| `timeout` | 30 秒 | 请求超时 |
| `connect_timeout` | 10 秒 | 连接超时 |

### 常用单位常量

```rust
use hydra_dl::constants::*;

let config = DownloadConfig::builder()
    .initial_chunk_size(10 * MB)  // 10 MB
    .min_speed_threshold(5 * MB)  // 5 MB/s
    .build()?;
```

## 🤝 贡献

欢迎贡献代码、报告问题或提出建议！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 🙏 致谢

- [Tokio](https://tokio.rs/) - 异步运行时
- [Reqwest](https://docs.rs/reqwest/) - HTTP 客户端

---

<div align="center">

**如果觉得这个项目有用，请给它一个 ⭐️！**

</div>

