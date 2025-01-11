use tokio;
use teloxide::{prelude::*, utils::command::BotCommands};
use dotenv::dotenv;
use lapin::{Connection, ConnectionProperties, options::{QueueDeclareOptions, BasicPublishOptions}, types::FieldTable, BasicProperties};
use url::Url;
use std::env;

extern crate pretty_env_logger;

// Define bot commands
#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "These are the supported commands:"
)]
enum Command {
    #[command(description = "display this text.")]
    Help,
    #[command(description = "start the bot.")]
    Start,
    #[command(description = "gets the given url.")]
    Get(String),
}


//static HLS_URL: &str = "https://digital-cdn.net/hls4/aWQ9MTc5MTM7MTM4OTg1MTQxMzszMzcwMjI3NTsxMDI3NzIwOzE3MzMwNzUxMTEmaD1rekdkODBKU2I5OGxRaW8wZE5SNTFnJmU9MTczMzE2MTUxMQ/1027720.m3u8?loc=nl";

#[tokio::main]
async fn main() {
    dotenv().ok();
    pretty_env_logger::init();
    
    // Connect to RabbitMQ
    let amqp_addr = env::var("AMQP_ADDR").expect("AMQP_ADDR must be set");
    let conn = Connection::connect(&amqp_addr, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");
    log::info!("Connected to RabbitMQ");
    
    // Create a channel and queue
    let channel = conn.create_channel().await.expect("Failed to create channel");
    let queue = "youtube_urls";
    channel.queue_declare(
        queue,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    ).await.expect("Failed to declare queue");
    log::info!("Created RabbitMQ queue: {}", queue);

    // Start the bot
    log::info!("Starting the bot...");
    let bot = Bot::from_env();

    let handler = Update::filter_message()
        .branch(
            dptree::entry()
                .filter_command::<Command>()
                .endpoint(answer)
        )
        .branch(
            dptree::entry()
                .endpoint(move |bot, msg| handle_message(bot, msg, channel.clone()))
        );

    Dispatcher::builder(bot, handler)
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn handle_message(bot: Bot, msg: Message, channel: lapin::Channel) -> ResponseResult<()> {
    if let Some(text) = msg.text() {
        log::info!("Received plain text message: {}", text);
        
        // Check if it's a YouTube URL
        if let Ok(url) = Url::parse(text) {
            if url.host_str() == Some("www.youtube.com") || url.host_str() == Some("youtube.com") {
                log::info!("Detected YouTube URL: {}", text);
                
                // Create JSON message with URL and user ID
                let message = serde_json::json!({
                    "url": text,
                    "user_id": msg.from.as_ref().unwrap().id.to_string()
                });
                let message_str = message.to_string();
                
                // Publish to RabbitMQ
                channel.basic_publish(
                    "",
                    "youtube_urls",
                    BasicPublishOptions::default(),
                    message_str.as_bytes(),
                    BasicProperties::default(),
                ).await.expect("Failed to publish message");
                
                bot.send_message(msg.chat.id, format!("YouTube URL added to queue: {}", text)).await?;
                return Ok(());
            }
        }
        
        bot.send_message(msg.chat.id, format!("You said: {}", text)).await?;
    }
    Ok(())
}

async fn answer(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
    match cmd {
        Command::Help => {
            bot.send_message(msg.chat.id, Command::descriptions().to_string()).await?;
        }
        Command::Start => {
            bot.send_message(msg.chat.id, "Welcome to the Rust Telegram bot!").await?;
        }
        Command::Get(text) => {
            log::info!("will get {}", text);
            bot.send_message(msg.chat.id, text).await?;
        }
    }
    Ok(())
}
