use std::env;
use actix_multipart::Multipart;
use actix_web::{get, post, web, App, Error, HttpResponse, HttpServer, Responder, middleware::Logger};
use futures::StreamExt;
use regex::Regex;
use serde_json::{json, Value};
use tera::{Context, Tera};
use csv;
use chardetng::EncodingDetector;
use encoding_rs::Encoding;
use env_logger;
use log::info;
use actix_files::NamedFile;
use actix_web::mime;

#[derive(Clone)]
struct AdsConfig {
    client_id: String,
    slot_top: String,
    slot_bottom: String,
}

async fn decode_to_utf8(bytes: &[u8]) -> String {
    let mut det = EncodingDetector::new();
    det.feed(bytes, true);
    let enc: &'static Encoding = det.guess(None, true);
    let (cow, _, _) = enc.decode(bytes);
    cow.into_owned()
}

async fn parse_multipart(
    payload: &mut Multipart,
) -> Result<
    (
        String, Vec<u8>, String,
        Vec<String>, Vec<String>, Vec<String>,
        Vec<String>, Vec<String>,
    ),
    Error,
> {
    let mut mode = String::new();
    let mut data = Vec::new();
    let mut primary_key = String::new();
    let mut ops = Vec::new();
    let mut patterns = Vec::new();
    let mut replaces = Vec::new();
    let mut targets = Vec::new();
    let mut vals = Vec::new();

    while let Some(field) = payload.next().await {
        let mut f = field?;
        match f.name() {
            "file" => {
                while let Some(chunk) = f.next().await {
                    data.extend_from_slice(&chunk?);
                }
            }
            "mode" => if let Some(c) = f.next().await {
                mode = String::from_utf8_lossy(&c?).to_string();
            },
            "primaryKey" => if let Some(c) = f.next().await {
                primary_key = String::from_utf8_lossy(&c?).to_string();
            },
            "condOp[]" => if let Some(c) = f.next().await {
                ops.push(String::from_utf8_lossy(&c?).to_string());
            },
            "condPattern[]" => if let Some(c) = f.next().await {
                patterns.push(String::from_utf8_lossy(&c?).to_string());
            },
            "condReplace[]" => if let Some(c) = f.next().await {
                replaces.push(String::from_utf8_lossy(&c?).to_string());
            },
            "condTargetType[]" => if let Some(c) = f.next().await {
                targets.push(String::from_utf8_lossy(&c?).to_string());
            },
            "condTargetValue[]" => if let Some(c) = f.next().await {
                vals.push(String::from_utf8_lossy(&c?).to_string());
            },
            _ => {}
        }
    }
    Ok((mode, data, primary_key, ops, patterns, replaces, targets, vals))
}

// CSV→JSON：先に正規表現フィルタ→プライマリキーでマップ or 配列
fn csv_to_json_with_primary_key(
    raw: &str,
    primary_key: &str,
    ops: &[String],
    patterns: &[String],
    replaces: &[String],
    targets: &[String],
    vals: &[String],
) -> String {
    let mut rdr = csv::Reader::from_reader(raw.as_bytes());
    let headers: Vec<String> = rdr
        .headers()
        .unwrap_or(&csv::StringRecord::new())
        .iter().map(String::from).collect();
    let mut records: Vec<Vec<String>> = rdr
        .records()
        .filter_map(|r| r.ok())
        .map(|rec| rec.iter().map(String::from).collect())
        .collect();

    let cnt = ops.len().min(patterns.len()).min(replaces.len())
        .min(targets.len()).min(vals.len());
    for i in 0..cnt {
        let op  = &ops[i];
        let pat = &patterns[i];
        if pat.is_empty() { continue; }
        let rep = &replaces[i];
        let tgt = &targets[i];
        let val = &vals[i];
        let re  = match Regex::new(pat) { Ok(r)=>r, Err(_) => continue };
        match tgt.as_str() {
            "all" => {
                for row in &mut records {
                    for cell in row.iter_mut() {
                        let out = match op.as_str() {
                            "extract" => re.find(cell).map(|m| m.as_str().to_string()).unwrap_or_default(),
                            "replace" => re.replace_all(cell, rep.as_str()).to_string(),
                            "split"   => re.split(cell).collect::<Vec<_>>().join(""),
                            _ => cell.clone(),
                        };
                        *cell = out;
                    }
                }
            },
            "column" => if let Ok(n)=val.parse::<usize>() {
                let idx = n.saturating_sub(1);
                for row in &mut records {
                    if idx < row.len() {
                        let old = row[idx].clone();
                        row[idx] = match op.as_str() {
                            "extract" => re.find(&old).map(|m|m.as_str().to_string()).unwrap_or_default(),
                            "replace" => re.replace_all(&old, rep).to_string(),
                            "split"   => re.split(&old).collect::<Vec<_>>().join(""),
                            _ => old,
                        };
                    }
                }
            },
            "header" => if let Some(idx)=headers.iter().position(|h|h==val) {
                for row in &mut records {
                    if idx<row.len() {
                        let old = row[idx].clone();
                        row[idx] = match op.as_str() {
                            "extract" => re.find(&old).map(|m|m.as_str().to_string()).unwrap_or_default(),
                            "replace" => re.replace_all(&old, rep).to_string(),
                            "split"   => re.split(&old).collect::<Vec<_>>().join(""),
                            _ => old,
                        };
                    }
                }
            },
            _ => {}
        }
    }

    if !primary_key.is_empty() {
        if let Some(idx)=headers.iter().position(|h|h==primary_key) {
            let mut map = serde_json::Map::new();
            for row in records {
                if idx<row.len() {
                    let key = row[idx].clone();
                    let mut obj = serde_json::Map::new();
                    for (i,h) in headers.iter().enumerate() {
                        if i==idx { continue; }
                        obj.insert(h.clone(), Value::String(row.get(i).cloned().unwrap_or_default()));
                    }
                    map.insert(key, Value::Object(obj));
                }
            }
            return Value::Object(map).to_string();
        }
    }

    let arr: Vec<Value> = records.into_iter().map(|row| {
        let mut obj = serde_json::Map::new();
        for (h,cell) in headers.iter().zip(row.into_iter()) {
            obj.insert(h.clone(), Value::String(cell));
        }
        Value::Object(obj)
    }).collect();
    serde_json::to_string_pretty(&arr).unwrap_or_default()
}

// JSON→CSV
fn json_to_csv(raw: &str) -> String {
    let v: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    let arr: Vec<Value> = match v {
        Value::Array(a) => a,
        Value::Object(map) => {
            map.into_iter().map(|(k, v)| {
                if let Value::Object(mut obj) = v {
                    obj.insert("key".to_string(), Value::String(k));
                    Value::Object(obj)
                } else {
                    let mut obj = serde_json::Map::new();
                    obj.insert("key".to_string(), Value::String(k));
                    obj.insert("value".to_string(), v);
                    Value::Object(obj)
                }
            }).collect()
        }
        _ => return String::new(),
    };
    if arr.is_empty() { return String::new(); }
    let headers: Vec<String> = {
        let mut set = std::collections::BTreeSet::new();
        for item in &arr {
            if let Some(obj) = item.as_object() {
                for key in obj.keys() {
                    set.insert(key.clone());
                }
            }
        }
        set.into_iter().collect()
    };
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record(&headers).ok();
    for item in arr {
        if let Some(obj) = item.as_object() {
            let row: Vec<String> = headers.iter().map(|h| {
                match obj.get(h) {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                }
            }).collect();
            wtr.write_record(&row).ok();
        }
    }
    String::from_utf8(wtr.into_inner().unwrap_or_default()).unwrap_or_default()
}

// JSON→JSON キー指定フィルタ
fn apply_jsonkey_filters(
    raw: &str,
    ops: &[String],
    patterns: &[String],
    replaces: &[String],
    targets: &[String],
    vals: &[String],
) -> String {
    let mut v: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    if let Some(arr)=v.as_array_mut() {
        let cnt = ops.len().min(patterns.len())
            .min(replaces.len()).min(targets.len()).min(vals.len());
        for i in 0..cnt {
            let op  = &ops[i];
            let pat = &patterns[i];
            if pat.is_empty() { continue; }
            let rep = &replaces[i];
            let tgt = &targets[i];
            let val = &vals[i];
            if tgt!="jsonKey" { continue; }
            let re = match Regex::new(pat) {Ok(r)=>r,Err(_) => continue};
            for obj in arr.iter_mut().filter_map(Value::as_object_mut) {
                if let Some(s)=obj.get(val).and_then(Value::as_str) {
                    let new = match op.as_str() {
                        "extract" => re.find(s).map(|m|m.as_str().to_string()).unwrap_or_default(),
                        "replace" => re.replace_all(s, rep).to_string(),
                        "split"   => re.split(s).collect::<Vec<_>>().join(""),
                        _ => s.to_string(),
                    };
                    obj.insert(val.clone(), Value::String(new));
                }
            }
        }
    }
    serde_json::to_string_pretty(&v).unwrap_or_else(|_| raw.to_string())
}

#[get("/")]
async fn index(
    tmpl: web::Data<Tera>,
    ads: web::Data<AdsConfig>,
) -> impl Responder {
    let mut ctx = Context::new();
    ctx.insert("mode", &"csv2json");
    ctx.insert("adsense_client_id", &ads.client_id);
    ctx.insert("adsense_slot_top", &ads.slot_top);
    ctx.insert("adsense_slot_bottom", &ads.slot_bottom);
    let html = tmpl.render("index.html", &ctx).unwrap_or_else(|e| e.to_string());
    HttpResponse::Ok().content_type("text/html; charset=utf-8").body(html)
}

#[post("/api/convert")]
async fn api_convert(mut payload: Multipart) -> Result<HttpResponse, Error> {
    let (mode, data, primary_key, ops, pats, reps, tgts, vals) = parse_multipart(&mut payload).await?;
    let text = decode_to_utf8(&data).await;
    let result = match mode.as_str() {
        "csv2json" => csv_to_json_with_primary_key(&text, &primary_key, &ops, &pats, &reps, &tgts, &vals),
        "json2csv" => json_to_csv(&text),
        "json2json" => apply_jsonkey_filters(&text, &ops, &pats, &reps, &tgts, &vals),
        _ => text,
    };
    Ok(HttpResponse::Ok().content_type("application/json").body(json!({ "result": result }).to_string()))
}

#[get("/ads.txt")]
async fn ads_txt() -> Result<NamedFile, actix_web::Error> {
    Ok(NamedFile::open("./static/ads.txt")?
        .set_content_type(mime::TEXT_PLAIN))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let port: u16 = env::var("PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(8080);
    let host = "0.0.0.0";
    info!("Server running at http://{}:{}", host, port);

    let ads = AdsConfig {
        client_id: env::var("ADSENSE_CLIENT_ID").unwrap_or_else(|_| "ca-pub-XXXXXXXXXXXXXXX".into()),
        slot_top: env::var("ADSENSE_SLOT_TOP").unwrap_or_else(|_| "1234567890".into()),
        slot_bottom: env::var("ADSENSE_SLOT_BOTTOM").unwrap_or_else(|_| "0987654321".into()),
    };

    let tera = Tera::new("templates/**/*").expect("テンプレート読み込み失敗");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(tera.clone()))
            .app_data(web::Data::new(ads.clone()))
            .service(ads_txt)
            .service(index)
            .service(api_convert)
    })
    .bind((host, port))?
    .run()
    .await
}
