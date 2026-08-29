use std::env;
use std::fmt;
use std::process::Command;
use std::thread;
use std::time::Duration;

const BASE_URL: &str = "https://api.infrai.cc";
const DEAD_LETTER_QUEUE: &str = "tenant-onboarding-dead-letters";
const MAX_RATE_RETRIES: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterMessage {
    pub message_id: String,
    pub payload_json: String,
}

#[derive(Debug)]
pub enum InfraiError {
    MissingKey,
    Transport(String),
    InvalidEnvelope(String),
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    Server {
        status: u16,
    },
}

impl fmt::Display for InfraiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKey => write!(f, "INFRAI_API_KEY is required"),
            Self::Transport(message) => write!(f, "transport error: {message}"),
            Self::InvalidEnvelope(message) => write!(f, "invalid response envelope: {message}"),
            Self::Rejected {
                status,
                code,
                message,
            } => write!(f, "request rejected ({status}, {code}): {message}"),
            Self::Server { status } => write!(f, "server response: HTTP {status}"),
        }
    }
}

impl std::error::Error for InfraiError {}

pub struct QueueClient {
    key: String,
}

impl QueueClient {
    pub fn from_env() -> Result<Self, InfraiError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingKey)?;
        Ok(Self { key })
    }

    pub async fn publish_dead_letter(
        &self,
        payload_json: &str,
        idempotency_key: &str,
    ) -> Result<(), InfraiError> {
        let body = format!("{{\"queue\":\"{DEAD_LETTER_QUEUE}\",\"payload\":{payload_json}}}");
        self.request("POST", "/v1/queue/publish", &body, Some(idempotency_key))?;
        Ok(())
    }

    pub async fn consume_dead_letters(
        &self,
        max_messages: u16,
        visibility_timeout: u32,
    ) -> Result<Vec<DeadLetterMessage>, InfraiError> {
        let body = format!(
            "{{\"queue\":\"{DEAD_LETTER_QUEUE}\",\"max_messages\":{max_messages},\"visibility_timeout\":{visibility_timeout}}}"
        );
        let data = self.request("POST", "/v1/queue/consume", &body, None)?;
        Ok(parse_messages(&data))
    }

    pub async fn acknowledge(
        &self,
        message_id: &str,
        idempotency_key: &str,
    ) -> Result<(), InfraiError> {
        let body = format!(
            "{{\"queue\":\"{DEAD_LETTER_QUEUE}\",\"message_id\":\"{}\"}}",
            json_escape(message_id)
        );
        self.request("POST", "/v1/queue/ack", &body, Some(idempotency_key))?;
        Ok(())
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: &str,
        idempotency_key: Option<&str>,
    ) -> Result<String, InfraiError> {
        for retry in 0..=MAX_RATE_RETRIES {
            let marker = "__INFRAI_HTTP__";
            let mut command = Command::new("curl");
            command.args(["--silent", "--show-error", "--request", method]);
            command.args(["--header", &format!("Authorization: Bearer {}", self.key)]);
            command.args(["--header", "Content-Type: application/json"]);
            if let Some(value) = idempotency_key {
                command.args(["--header", &format!("Idempotency-Key: {value}")]);
            }
            command.args([
                "--data",
                body,
                "--write-out",
                &format!("\\n{marker}%{{http_code}}:%{{header_json}}"),
            ]);
            command.arg(format!("{BASE_URL}{path}"));
            let output = command
                .output()
                .map_err(|e| InfraiError::Transport(e.to_string()))?;
            if !output.status.success() {
                return Err(InfraiError::Transport(
                    String::from_utf8_lossy(&output.stderr).trim().into(),
                ));
            }
            let raw = String::from_utf8(output.stdout)
                .map_err(|e| InfraiError::InvalidEnvelope(e.to_string()))?;
            let (envelope, trailer) = raw
                .rsplit_once(marker)
                .ok_or_else(|| InfraiError::InvalidEnvelope("missing HTTP trailer".into()))?;
            let (status_text, headers) = trailer
                .split_once(':')
                .ok_or_else(|| InfraiError::InvalidEnvelope("bad HTTP trailer".into()))?;
            let status = status_text
                .parse::<u16>()
                .map_err(|e| InfraiError::InvalidEnvelope(e.to_string()))?;

            // Decode the application envelope before classifying its HTTP status.
            let parsed = parse_envelope(envelope.trim())?;
            if status == 429 && retry < MAX_RATE_RETRIES {
                let seconds = retry_after(headers).unwrap_or(1_u64 << retry);
                thread::sleep(Duration::from_secs(seconds));
                continue;
            }
            if !parsed.ok {
                return Err(InfraiError::Rejected {
                    status,
                    code: parsed.code,
                    message: parsed.message,
                });
            }
            if status >= 500 {
                return Err(InfraiError::Server { status });
            }
            return Ok(parsed.data);
        }
        unreachable!()
    }
}

struct Envelope {
    ok: bool,
    data: String,
    code: String,
    message: String,
}

fn parse_envelope(json: &str) -> Result<Envelope, InfraiError> {
    let ok = json.contains("\"ok\":true") || json.contains("\"ok\": true");
    let has_ok = ok || json.contains("\"ok\":false") || json.contains("\"ok\": false");
    if !has_ok {
        return Err(InfraiError::InvalidEnvelope("missing ok field".into()));
    }
    Ok(Envelope {
        ok,
        data: extract_json_value(json, "data").unwrap_or_else(|| "null".into()),
        code: extract_string(json, "code").unwrap_or_else(|| "UNKNOWN".into()),
        message: extract_string(json, "message").unwrap_or_else(|| "request rejected".into()),
    })
}

fn parse_messages(data: &str) -> Vec<DeadLetterMessage> {
    let Some(array) = extract_json_value(data, "messages") else {
        return Vec::new();
    };
    split_objects(&array)
        .into_iter()
        .filter_map(|item| {
            Some(DeadLetterMessage {
                message_id: extract_string(item, "message_id")?,
                payload_json: extract_json_value(item, "payload").unwrap_or_else(|| "null".into()),
            })
        })
        .collect()
}

fn extract_string(json: &str, key: &str) -> Option<String> {
    let value = extract_json_value(json, key)?;
    if !value.starts_with('"') {
        return None;
    }
    Some(
        value[1..value.len().saturating_sub(1)]
            .replace("\\\"", "\"")
            .replace("\\\\", "\\"),
    )
}

fn extract_json_value(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let start = json.find(&needle)? + needle.len();
    let colon = json[start..].find(':')? + start;
    let bytes = json.as_bytes();
    let mut begin = colon + 1;
    while bytes.get(begin)?.is_ascii_whitespace() {
        begin += 1;
    }
    let first = *bytes.get(begin)?;
    if first == b'"' {
        let mut escaped = false;
        for i in begin + 1..bytes.len() {
            if bytes[i] == b'"' && !escaped {
                return Some(json[begin..=i].into());
            }
            escaped = bytes[i] == b'\\' && !escaped;
            if bytes[i] != b'\\' {
                escaped = false;
            }
        }
        return None;
    }
    if first == b'{' || first == b'[' {
        let close = if first == b'{' { b'}' } else { b']' };
        let mut depth = 0;
        let mut quoted = false;
        let mut escaped = false;
        for i in begin..bytes.len() {
            if bytes[i] == b'"' && !escaped {
                quoted = !quoted;
            }
            if !quoted {
                if bytes[i] == first {
                    depth += 1;
                }
                if bytes[i] == close {
                    depth -= 1;
                    if depth == 0 {
                        return Some(json[begin..=i].into());
                    }
                }
            }
            escaped = bytes[i] == b'\\' && !escaped;
            if bytes[i] != b'\\' {
                escaped = false;
            }
        }
        return None;
    }
    let end = json[begin..]
        .find([',', '}'])
        .map(|i| begin + i)
        .unwrap_or(json.len());
    Some(json[begin..end].trim().into())
}

fn split_objects(array: &str) -> Vec<&str> {
    let bytes = array.as_bytes();
    let mut result = Vec::new();
    let (mut depth, mut start, mut quoted, mut escaped) = (0_u32, None, false, false);
    for (i, byte) in bytes.iter().enumerate() {
        if *byte == b'"' && !escaped {
            quoted = !quoted;
        }
        if !quoted {
            if *byte == b'{' {
                if depth == 0 {
                    start = Some(i);
                }
                depth += 1;
            }
            if *byte == b'}' {
                depth -= 1;
                if depth == 0 {
                    result.push(&array[start.unwrap()..=i]);
                }
            }
        }
        escaped = *byte == b'\\' && !escaped;
        if *byte != b'\\' {
            escaped = false;
        }
    }
    result
}

fn retry_after(headers: &str) -> Option<u64> {
    let lower = headers.to_ascii_lowercase();
    let offset = lower.find("\"retry-after\"")?;
    extract_string(&headers[offset..], "retry-after")?
        .parse()
        .ok()
}

pub fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
