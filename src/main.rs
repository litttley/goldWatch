use serde::Deserialize;
use reqwest::Client;
use tokio::time::Duration;
use std::collections::HashMap;
use futures::stream::{self, StreamExt};
use bigdecimal::{BigDecimal, ToPrimitive};
use std::thread;
use sqlx::mysql::MySqlPool;
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use colored::*;
use lettre::smtp::authentication::{Credentials, Mechanism};
use lettre::smtp::ConnectionReuseParameters;
use lettre::{SmtpClient, Transport};
use lettre_email::Email;
use std::ops::Deref;


const PARALLEL_REQUESTS: usize = 2; //控制并行请求数量，太大容易内存不足


lazy_static! {
    static ref FOO : AsyncOnce<MySqlPool> = AsyncOnce::new(async{

            let database_url = dotenv::var("DATABASE_URL").expect("数据库连接没有设置!");
            let pool =  sqlx::MySqlPool::builder().
                max_size(100).// 连接池上限
                min_size(50).// 连接池下限
                connect_timeout(Duration::from_secs(10)).// 连接超时时间
                max_lifetime(Duration::from_secs(1800)).// 所有连接的最大生命周期
                idle_timeout(Duration::from_secs(600)).// 空闲连接的生命周期
                build(database_url.as_str()).await;
                    pool.expect("数据库连接池创建失败!")
    });




}

lazy_static! {
    static ref MAILMAP: HashMap<&'static str, String> = {

        let mail_server_host = dotenv::var("mail_server_host").expect("mail_server_host must be set");
        let mail_server_port = dotenv::var("mail_server_port").expect("mail_server_port must be set");
        let mail_server_username = dotenv::var("mail_server_username").expect("mail_server_username must be set");
        let mail_server_password = dotenv::var("mail_server_password").expect("mail_server_password must be set");
        let mail_from_addr = dotenv::var("mail_from_addr").expect("mail_from_addr must be set");
        let mail_to_addr = dotenv::var("mail_to_addr").expect("mail_to_addr must be set");
        let mail_text = dotenv::var("mail_text").expect("mail_text must be set");
        let mail_title = dotenv::var("mail_title").expect("mail_title must be set");
        let loop_time = dotenv::var("loop_time").expect("mail_title must be set");

        let mut m = HashMap::new();
        m.insert("mail_server_host", mail_server_host);
        m.insert("mail_server_port", mail_server_port);
        m.insert("mail_server_username", mail_server_username);
        m.insert("mail_server_password", mail_server_password);
        m.insert("mail_from_addr", mail_from_addr);
        m.insert("mail_to_addr", mail_to_addr);
        m.insert("mail_text", mail_text);
        m.insert("mail_title", mail_title);

        m
    };
}

#[derive(Deserialize, Debug, Clone)]
struct JsonResult {
    status: i64,
    data: Vec<GlodInfo>,

}


#[derive(Deserialize, Debug, Clone)]
struct GlodInfo {
    CurPrice: String,
    Variety: String,

}


#[derive(Deserialize, Debug)]
struct GlodWatch {
    id: i64,
    code: String,
    remind_price: BigDecimal,
    remind_type: String,
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    let urls = vec![
        "http://m.cmbchina.com/api/rate/getgoldratedetail/?no=AUTD",
    ];

    loop {
        let is_true = check_info().await;
        if !is_true {
            println!("{}!","没有要监听的数据,请到数据库中设置.....".red().bold());
            thread::sleep(Duration::from_millis(5000));

            continue;
        }


        let bodies = stream::iter(urls.clone())
            .map(|url| {
                let client = &client;
                async move {
                    let resp = client
                        .get(url)
                        .timeout(Duration::from_secs(5))
                        .send()
                        .await?;
                    resp.json::<JsonResult>().await
                }
            }).buffer_unordered(PARALLEL_REQUESTS);

        bodies.for_each(|result| async {
            match result {
                Ok(result) => {
                    println!("{}", "接口调用成功".green().italic());
                   // println!("{:#?}", result);
                    parse_stock_data(result).await
                }
                Err(e) => println!("{}{}", "接口调用失败".red().italic(), e),
            }
        })
            .await;

       let loop_time =MAILMAP.get("loop_time").unwrap_or(&"60".to_string()).clone();
       let secs =  loop_time.parse::<u64>().unwrap_or(60);
        thread::sleep(Duration::from_secs(secs));
    }
}


async fn check_info() -> bool {
    let pool = FOO.get().await;

    let watch: sqlx::Result<Vec<GlodWatch>> = sqlx::query_as!(
        GlodWatch,
        r#"
         select id,code,remind_price ,remind_type from setting where is_closed=? and code='glod'
                "#,
        0
    ).fetch_all(&pool).await;

    if let Ok(glod_watch_array) = watch {
        if glod_watch_array.len() > 0 {
            true
        } else {
            false
        }
    } else {
        false
    }
}


async fn parse_stock_data(result: JsonResult) {
    let cur_price: String = result.data[0].CurPrice.clone();
    let code = result.data[0].Variety.clone();
    let cur_price = cur_price.parse::<f64>().unwrap_or(0.00);

    let pool = FOO.get().await;
    let watch: sqlx::Result<Vec<GlodWatch>> = sqlx::query_as!(
        GlodWatch,
        r#"
        select id,code,remind_price,remind_type from setting where is_closed=? and code='glod'
                "#,
        0

    ).fetch_all(&pool).await;

    if let Ok(glod_watch_array) = watch {
        for stock_watch in glod_watch_array {
          //  let code = stock_watch.code;
            let remind_price = stock_watch.remind_price;
            let remind_type = stock_watch.remind_type;

            print!("{}    ", "实    时".cyan().bold());
            print!("{}  ", code);
            println!("{:?}", cur_price);
            if remind_type.eq("0") {
                print!("{}    ", "卖出提醒".cyan().bold());
            } else {
                print!("{}    ", "买入提醒".cyan().bold());
            }

            print!("{}  ", code);
            println!("{:?}", remind_price.to_f64().unwrap_or(0.00));

            if remind_type.eq("0") && cur_price >= remind_price.to_f64().unwrap_or(0.00) {
                let mail_content = format!("黄金:{}    当前价格{}", code, cur_price);
                let is_true = send_mail(mail_content, "卖出提醒").await;

                if is_true {
                    let sql = r#"update setting set is_closed = ? where  code='glod'  "#;
                    let _affect_rows = sqlx::query(sql)
                        .bind(1)
                        .execute(&pool)
                        .await;
                }
            }
            if remind_type.eq("1") && cur_price <= remind_price.to_f64().unwrap_or(0.00) {
                let mail_content = format!("黄金:{}    当前价格{}", code, cur_price);
                let is_true = send_mail(mail_content, "买入提醒").await;

                if is_true {
                    let sql = r#"update setting set is_closed = ? where  code='glod'   "#;
                    let _affect_rows = sqlx::query(sql)
                        .bind(1)
                        .execute(&pool)
                        .await;
                }
            }
        }
    }
}

async fn send_mail(mail_content: String, mail_title: &str) -> bool {
    let email = Email::builder()
        .to(MAILMAP.get("mail_to_addr").unwrap().as_str())
        .from(MAILMAP.get("mail_from_addr").unwrap().as_str())
        .subject(mail_title)
        .text(mail_content)
        .build()
        .unwrap();

    let mut mailer = SmtpClient::new_simple(MAILMAP.get("mail_server_host").unwrap())
        .unwrap()
        .smtp_utf8(true)
        .credentials(Credentials::new(
            MAILMAP.get("mail_server_username").unwrap().to_string(),
            MAILMAP.get("mail_server_password").unwrap().to_string(),
        ))
        .authentication_mechanism(Mechanism::Plain)
        .connection_reuse(ConnectionReuseParameters::ReuseUnlimited)
        .transport();

    // 发送邮件
    let result = mailer.send(email.into());
    mailer.close();

    if result.is_ok() {
        println!("{}", "邮件通知发送成功!!!!".white().bold());
        true
    } else {
        println!("{}", "发送失败".red().blink());
        false
    }
}
