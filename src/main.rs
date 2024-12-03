use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::default_provider::region::DefaultRegionChain;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::Client;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use std::error::Error;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    profile: String,
    #[arg(short, long)]
    bucket: String,
    #[arg(long)]
    prefix: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let bucket = args.bucket;
    let profile = args.profile;
    let prefix = args.prefix;

    println!("bucket: {}", bucket);
    println!("profile: {}\n", profile);
    println!("prefix: {}\n", prefix);

    let region = DefaultRegionChain::builder()
        .profile_name(&profile)
        .build()
        .region()
        .await;

    let creds = DefaultCredentialsChain::builder()
        .profile_name(&profile)
        .region(region.clone())
        .build()
        .await;

    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .credentials_provider(creds)
        .region(region)
        .load()
        .await;

    let client = Client::new(&config);

    let mut timestamps: Vec<DateTime<Utc>> = Vec::new();

    let mut continuation_token = None;

    loop {
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await?;

        if let Some(contents) = resp.contents {
            for object in contents {
                if let Some(last_modified) = object.last_modified {
                    let last_modified_str = last_modified.to_string();

                    let datetime =
                        DateTime::parse_from_rfc3339(&last_modified_str)?.with_timezone(&Utc);

                    timestamps.push(datetime);
                }
            }
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    if timestamps.len() < 2 {
        println!("Not enough timestamps to calculate average.");
        return Ok(());
    }

    timestamps.sort();

    let mut time_diffs: Vec<Duration> = Vec::new();
    for window in timestamps.windows(2) {
        if let [prev, next] = window {
            let duration = *next - *prev;
            time_diffs.push(duration);
        }
    }

    let total_duration = time_diffs.iter().fold(Duration::zero(), |acc, x| acc + *x);

    let avg_duration = total_duration / (time_diffs.len() as i32);
    let total_millis = total_duration.num_milliseconds();

    let hours = total_millis / (1000 * 60 * 60);
    let remaining_millis = total_millis % (1000 * 60 * 60);

    let minutes = remaining_millis / (1000 * 60);
    let remaining_millis = remaining_millis % (1000 * 60);

    let seconds = remaining_millis / 1000;
    let milliseconds = remaining_millis % 1000;

    println!("Average time between timestamps: {:?}", avg_duration);
    println!(
        "Total time for {:?} files: {}h {}m {}s {}ms",
        time_diffs.len(),
        hours,
        minutes,
        seconds,
        milliseconds
    );

    Ok(())
}
