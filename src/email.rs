use resend_rs::types::CreateEmailBaseOptions;
use resend_rs::{Resend, Result};
use dotenvy::dotenv;
use std::env;
use crate::model::{WearReading, WearResult};

pub async fn send_email(to: &str, wear_reading: &WearReading, wear_result: &WearResult) -> Result<()> {
    dotenv().ok();
    let resend = Resend::new(env::var("RESEND_API_KEY").unwrap().as_str());

    let from = env::var("RESEND_FROM_EMAIL").unwrap();
    let to = [to];

    let wear_readable = match wear_result {
        WearResult::Nominal => "通常",
        WearResult::Warning => "警告",
        WearResult::Critical => "危険",
    };

    let email = CreateEmailBaseOptions::new(from, to, "摩耗検知アラート".to_string())
      .with_html(format!("<p>{}に設置されている{}の摩耗状態は<strong>{}</strong>です。</p>", wear_reading.facility_name, wear_reading.equipment_id, wear_readable).as_str());

    let _email = resend.emails.send(email).await?;
    println!("{:?}", _email);

    Ok(())
}