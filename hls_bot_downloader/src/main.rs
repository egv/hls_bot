use lapin::{Connection, ConnectionProperties};
use futures_lite::stream::StreamExt;
use lapin::options::{BasicConsumeOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use std::process::Command;
use std::str;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use chrono::Utc;

fn append_to_rss_feed(user_id: &str, title: &str, uploader: &str, video_file: &str, url: &str, metadata: &Value) {
    let duration = metadata["duration"].as_u64().unwrap_or(0);
    let rss_item = format!(r#"
    <item>
        <title>{}</title>
        <itunes:author>{}</itunes:author>
        <itunes:summary>{}</itunes:summary>
        <enclosure url="{}" type="video/mp4" length="{}"/>
        <guid>{}</guid>
        <pubDate>{}</pubDate>
        <itunes:duration>{}</itunes:duration>
        <itunes:explicit>no</itunes:explicit>
    </item>
    "#, 
    title,
    uploader,
    title, // Using title as summary
    video_file,
    metadata["filesize"].as_u64().unwrap_or(0), // File size
    url,
    Utc::now().to_rfc2822(),
    duration);
    
    // Update RSS feed
    let rss_path = format!("{}.rss", user_id);
    let mut rss_content = if fs::metadata(&rss_path).is_ok() {
        fs::read_to_string(&rss_path).expect("Failed to read RSS feed")
    } else {
        String::from(r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
    <title>YouTube Podcast</title>
    <description>Downloaded YouTube Videos as Podcast</description>
    <language>en-us</language>
    <itunes:author>YouTube Downloader</itunes:author>
    <itunes:summary>YouTube videos converted to podcast format</itunes:summary>
    <itunes:owner>
        <itunes:name>YouTube Downloader</itunes:name>
        <itunes:email>noreply@example.com</itunes:email>
    </itunes:owner>
    <itunes:explicit>no</itunes:explicit>
    <itunes:category text="Technology"/>
    <itunes:image href="https://example.com/podcast.jpg"/>
</channel>
</rss>"#)
    };
    
    // Insert new item after <channel> tag
    if let Some(pos) = rss_content.find("</channel>") {
        rss_content.insert_str(pos, &rss_item);
    }
    
    // Write updated RSS
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&rss_path)
        .expect("Failed to open RSS file");
    file.write_all(rss_content.as_bytes())
        .expect("Failed to write RSS file");
}

#[tokio::main]
async fn main() {
    // Connect to RabbitMQ
    let conn = Connection::connect(
        "amqp://127.0.0.1:5672/%2f",
        ConnectionProperties::default(),
    )
    .await
    .expect("Failed to connect to RabbitMQ");

    // Create a channel
    let channel = conn.create_channel().await.expect("Failed to create channel");

    // Declare the queue
    let _queue = channel
        .queue_declare(
            "youtube_urls",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    // Create consumer
    let mut consumer = channel
        .basic_consume(
            "youtube_urls",
            "youtube_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to create consumer");

    println!("Waiting for YouTube URLs...");

    // Process messages
    while let Some(Ok(delivery)) = consumer.next().await {
            if let Ok(message) = str::from_utf8(&delivery.data) {
                let msg_json: Value = serde_json::from_str(message)
                    .expect("Failed to parse message JSON");
                
                let url = msg_json["url"].as_str().expect("Missing url in message");
                let user_id = msg_json["user_id"].as_str().expect("Missing user_id in message");
                
                println!("Processing URL: {} for user: {}", url, user_id);
                
                // Execute youtube-dl to get metadata
                let output = Command::new("youtube-dl")
                    .arg("-J")  // Output JSON metadata
                    .arg(url)
                    .output()
                    .expect("Failed to execute youtube-dl");
                
                if output.status.success() {
                    let metadata = str::from_utf8(&output.stdout)
                        .expect("Invalid UTF-8 in youtube-dl output");
                    println!("Metadata: {}", metadata);
                    
                    // Download the video
                    let download_output = Command::new("youtube-dl")
                        .arg("-o")  // Output template
                        .arg("%(title)s.%(ext)s")  // Save as title.ext
                        .arg(url)
                        .output()
                        .expect("Failed to execute youtube-dl download");
                    
                    if download_output.status.success() {
                        println!("Successfully downloaded video: {}", url);
                        
                        // Parse metadata
                        let metadata: Value = serde_json::from_str(metadata)
                            .expect("Failed to parse metadata JSON");
                        
                        // Create RSS item
                        let title = metadata["title"].as_str().unwrap_or("Untitled");
                        let uploader = metadata["uploader"].as_str().unwrap_or("Unknown");
                        let video_file = format!("{}.{}", 
                            metadata["title"].as_str().unwrap_or("video"),
                            metadata["ext"].as_str().unwrap_or("mp4"));
                        
                        append_to_rss_feed(user_id, title, uploader, &video_file, url, &metadata);
                        println!("Added video to RSS feed: {}", title);
                    } else {
                        eprintln!("Error downloading video {}: {}", url, 
                            str::from_utf8(&download_output.stderr).unwrap_or("Unknown error"));
                    }
                } else {
                    eprintln!("Error getting metadata for {}: {}", url, str::from_utf8(&output.stderr).unwrap_or("Unknown error"));
                }
                
                // Acknowledge the message
                delivery.ack(lapin::options::BasicAckOptions::default())
                    .await
                    .expect("Failed to ack message");
            }
        }
    }
