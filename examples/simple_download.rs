use hydra_dl::download_file;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    let save_path = download_file("https://example.com/file.zip", ".").await?;
    println!("文件已保存到: {:?}", save_path);
    Ok(())
}
