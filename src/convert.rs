use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde_json::{Map, Value, json};

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
}

#[derive(Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
}
impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((at, separator_len)) = event_boundary(&self.buffer) {
            let raw = String::from_utf8_lossy(&self.buffer[..at]).into_owned();
            self.buffer.drain(..at + separator_len);
            if let Some(event) = parse_sse_event(&raw) {
                events.push(event);
            }
        }
        events
    }
    pub fn finish(&mut self) -> Option<SseEvent> {
        let raw = String::from_utf8_lossy(&std::mem::take(&mut self.buffer)).into_owned();
        (!raw.trim().is_empty())
            .then(|| parse_sse_event(&raw))
            .flatten()
    }
}
fn event_boundary(value: &[u8]) -> Option<(usize, usize)> {
    let lf = value
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .map(|i| (i, 2));
    let crlf = value
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .map(|i| (i, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        _ => None,
    }
}
fn parse_sse_event(raw: &str) -> Option<SseEvent> {
    let mut event = "message".to_owned();
    let mut data = Vec::new();
    for line in raw.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = line
            .split_once(':')
            .map_or((line, ""), |(a, b)| (a, b.strip_prefix(' ').unwrap_or(b)));
        match field {
            "event" => event = value.into(),
            "data" => data.push(value),
            _ => {}
        }
    }
    (!data.is_empty()).then(|| SseEvent {
        event,
        data: data.join("\n"),
    })
}

pub fn normalize_responses_request(body: &Value) -> Value {
    let mut normalized = body.as_object().cloned().unwrap_or_default();
    if let Some(input) = normalized
        .get("input")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        normalized.insert(
            "input".into(),
            json!([{ "role": "user", "content": input }]),
        );
    }
    normalized.insert("store".into(), Value::Bool(false));
    normalized.insert("stream".into(), Value::Bool(true));
    for key in [
        "max_output_tokens",
        "temperature",
        "top_p",
        "metadata",
        "user",
    ] {
        normalized.remove(key);
    }
    Value::Object(normalized)
}

pub fn reasoning_effort(body: &Value) -> Option<String> {
    body.pointer("/reasoning/effort")
        .and_then(Value::as_str)
        .or_else(|| body.get("reasoningEffort").and_then(Value::as_str))
        .or_else(|| body.get("reasoning_effort").and_then(Value::as_str))
        .map(str::to_owned)
}

pub fn build_responses_request(chat: &Value) -> Result<Value, Vec<String>> {
    let mut instructions = Vec::new();
    let mut input = Vec::new();
    let mut unsupported = Vec::new();
    for message in chat
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user");
        let content = extract_text(message.get("content"), &mut unsupported);
        if role == "system" || role == "developer" {
            if !content.is_empty() {
                instructions.push(content);
            }
            continue;
        }
        if role == "tool" {
            input.push(json!({ "type": "function_call_output", "call_id": message.get("tool_call_id").or_else(|| message.get("call_id")).and_then(Value::as_str).unwrap_or("unknown_call"), "output": content }));
            continue;
        }
        if !content.is_empty() {
            input.push(json!({ "role": if role == "assistant" { "assistant" } else { "user" }, "content": content }));
        }
        if role == "assistant" {
            for call in message
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if call.get("type").and_then(Value::as_str) != Some("function") {
                    continue;
                }
                input.push(json!({
                    "type": "function_call",
                    "call_id": call.get("id").and_then(Value::as_str).map(str::to_owned).unwrap_or_else(|| random_id("call")),
                    "name": call.pointer("/function/name").and_then(Value::as_str).unwrap_or("unknown_function"),
                    "arguments": call.pointer("/function/arguments").and_then(Value::as_str).unwrap_or("{}")
                }));
            }
        }
    }
    if !unsupported.is_empty() {
        return Err(unsupported);
    }
    let mut body = Map::new();
    body.insert(
        "model".into(),
        chat.get("model").cloned().unwrap_or(Value::Null),
    );
    body.insert("input".into(), Value::Array(input));
    body.insert(
        "stream".into(),
        chat.get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            .into(),
    );
    body.insert("store".into(), false.into());
    if !instructions.is_empty() {
        body.insert("instructions".into(), instructions.join("\n\n").into());
    }
    if let Some(tools) = chat.get("tools").and_then(Value::as_array) {
        body.insert(
            "tools".into(),
            Value::Array(tools.iter().map(convert_tool).collect()),
        );
    }
    if let Some(choice) = chat.get("tool_choice") {
        body.insert("tool_choice".into(), convert_tool_choice(choice));
    }
    for key in ["parallel_tool_calls", "reasoning"] {
        if let Some(value) = chat.get(key) {
            body.insert(key.into(), value.clone());
        }
    }
    Ok(Value::Object(body))
}

fn extract_text(content: Option<&Value>, unsupported: &mut Vec<String>) -> String {
    match content {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| match part {
                Value::String(text) => Some(text.clone()),
                Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
                    Some("text" | "input_text" | "output_text") => {
                        Some(obj.get("text").and_then(Value::as_str).unwrap_or("").into())
                    }
                    other => {
                        unsupported.push(other.unwrap_or("unknown").into());
                        None
                    }
                },
                _ => None,
            })
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Some(other) => {
            unsupported.push(
                match other {
                    Value::Bool(_) => "boolean",
                    Value::Number(_) => "number",
                    Value::Object(_) => "object",
                    _ => "unknown",
                }
                .into(),
            );
            String::new()
        }
    }
}
fn convert_tool(tool: &Value) -> Value {
    if tool.get("type").and_then(Value::as_str) != Some("function") {
        return tool.clone();
    }
    json!({ "type": "function", "name": tool.pointer("/function/name").cloned().unwrap_or(Value::Null), "description": tool.pointer("/function/description").cloned().unwrap_or(Value::Null), "parameters": tool.pointer("/function/parameters").cloned().unwrap_or_else(|| json!({ "type": "object", "properties": {} })) })
}
fn convert_tool_choice(choice: &Value) -> Value {
    if choice.get("type").and_then(Value::as_str) == Some("function")
        && let Some(name) = choice.pointer("/function/name")
    {
        return json!({ "type": "function", "name": name });
    }
    choice.clone()
}

pub fn normalize_models(upstream: &Value) -> Value {
    if upstream.get("object").and_then(Value::as_str) == Some("list")
        && upstream.get("data").is_some_and(Value::is_array)
    {
        return upstream.clone();
    }
    let models: &[Value] = upstream
        .get("models")
        .and_then(Value::as_array)
        .or_else(|| upstream.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let data = models.iter().filter_map(|model| {
        let id = model.get("id").or_else(|| model.get("slug")).or_else(|| model.get("name"))?.clone();
        Some(json!({ "id": id, "object": "model", "created": model.get("created").cloned().unwrap_or(0.into()), "owned_by": model.get("owned_by").cloned().unwrap_or("chatgpt".into()) }))
    }).collect::<Vec<_>>();
    json!({ "object": "list", "data": data })
}

pub fn collect_responses_events(
    events: impl IntoIterator<Item = SseEvent>,
) -> Result<Value, String> {
    let mut final_response = Map::new();
    let mut output: Vec<Value> = Vec::new();
    for event in events {
        if event.data.is_empty() || event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        let kind = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(&event.event);
        if let Some(response) = payload.get("response").and_then(Value::as_object) {
            final_response.extend(response.clone());
        }
        match kind {
            "response.output_item.added" => {
                if let Some(item) = payload.get("item") {
                    let index = payload
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(output.len() as u64) as usize;
                    set_index(&mut output, index, item.clone());
                }
            }
            "response.output_item.done" => {
                if let Some(item) = payload.get("item") {
                    let index = payload
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize;
                    set_index(&mut output, index, item.clone());
                }
            }
            "response.output_text.delta" => {
                let oi = payload
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize;
                let ci = payload
                    .get("content_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize;
                ensure_message(&mut output, oi);
                let item = output[oi].as_object_mut().unwrap();
                let content = item
                    .entry("content")
                    .or_insert_with(|| Value::Array(vec![]))
                    .as_array_mut()
                    .unwrap();
                while content.len() <= ci {
                    content.push(json!({ "type": "output_text", "text": "" }));
                }
                let part = content[ci].as_object_mut().unwrap();
                let text = part
                    .entry("text")
                    .or_insert_with(|| Value::String(String::new()))
                    .as_str()
                    .unwrap_or("")
                    .to_owned();
                part.insert(
                    "text".into(),
                    format!(
                        "{text}{}",
                        payload.get("delta").and_then(Value::as_str).unwrap_or("")
                    )
                    .into(),
                );
            }
            "response.function_call_arguments.delta" => {
                let oi = payload
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize;
                while output.len() <= oi {
                    output.push(Value::Null);
                }
                if output[oi].is_null() {
                    output[oi] = json!({ "type": "function_call", "arguments": "" });
                }
                let item = output[oi].as_object_mut().unwrap();
                let args = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                item.insert(
                    "arguments".into(),
                    format!(
                        "{args}{}",
                        payload.get("delta").and_then(Value::as_str).unwrap_or("")
                    )
                    .into(),
                );
            }
            "response.failed" => {
                return Err(payload
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Upstream response failed")
                    .into());
            }
            _ => {}
        }
    }
    if !final_response
        .get("output")
        .is_some_and(|v| v.as_array().is_some_and(|a| !a.is_empty()))
    {
        final_response.insert(
            "output".into(),
            Value::Array(output.into_iter().filter(|v| !v.is_null()).collect()),
        );
    }
    Ok(Value::Object(final_response))
}
fn set_index(values: &mut Vec<Value>, index: usize, value: Value) {
    while values.len() <= index {
        values.push(Value::Null);
    }
    values[index] = value;
}
fn ensure_message(output: &mut Vec<Value>, index: usize) {
    while output.len() <= index {
        output.push(Value::Null);
    }
    if output[index].is_null() {
        output[index] = json!({ "type": "message", "role": "assistant", "content": [] });
    }
}

pub fn response_to_chat(response: &Value, requested_model: &str) -> Value {
    let mut content = String::new();
    let mut calls = Vec::new();
    for item in response
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => for part in item.get("content").and_then(Value::as_array).into_iter().flatten() {
                if matches!(part.get("type").and_then(Value::as_str), Some("output_text" | "text")) { content.push_str(part.get("text").and_then(Value::as_str).unwrap_or("")); }
            },
            Some("function_call") => calls.push(json!({ "id": item.get("call_id").or_else(|| item.get("id")).cloned().unwrap_or_else(|| random_id("call").into()), "type": "function", "function": { "name": item.get("name").cloned().unwrap_or("".into()), "arguments": item.get("arguments").cloned().unwrap_or("{}".into()) } })),
            _ => {}
        }
    }
    let has_calls = !calls.is_empty();
    let mut message = json!({ "role": "assistant", "content": if has_calls && content.is_empty() { Value::Null } else { content.into() } });
    if has_calls {
        message["tool_calls"] = calls.into();
    }
    json!({
        "id": response.get("id").cloned().unwrap_or_else(|| random_id("chatcmpl").into()), "object": "chat.completion",
        "created": chrono::Utc::now().timestamp(), "model": response.get("model").cloned().unwrap_or_else(|| requested_model.into()),
        "choices": [{ "index": 0, "message": message, "finish_reason": if has_calls { "tool_calls" } else { "stop" } }],
        "usage": chat_usage(response.get("usage"))
    })
}
pub fn chat_usage(usage: Option<&Value>) -> Value {
    let Some(usage) = usage else {
        return Value::Null;
    };
    let prompt = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    json!({ "prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": usage.get("total_tokens").and_then(Value::as_u64).unwrap_or(prompt + completion) })
}
pub fn random_id(prefix: &str) -> String {
    let mut bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut bytes);
    format!("{prefix}_{}", URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sse_handles_crlf_and_split_chunks() {
        let mut d = SseDecoder::default();
        assert!(d.push(b"event: ping\r\nda").is_empty());
        let e = d.push(b"ta: one\r\ndata: two\r\n\r\n");
        assert_eq!(e[0].event, "ping");
        assert_eq!(e[0].data, "one\ntwo");
    }
    #[test]
    fn sse_preserves_utf8_split_across_chunks() {
        let source = "data: café\n\n".as_bytes();
        let split = source.iter().position(|byte| *byte == 0xc3).unwrap() + 1;
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(&source[..split]).is_empty());
        assert_eq!(decoder.push(&source[split..])[0].data, "café");
    }
    #[test]
    fn converts_chat_tools() {
        let input = json!({"model":"gpt", "messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"go","arguments":"{}"}}]},{"role":"tool","tool_call_id":"c1","content":"ok"}]});
        let out = build_responses_request(&input).unwrap();
        assert_eq!(out.pointer("/input/0/type"), Some(&json!("function_call")));
        assert_eq!(
            out.pointer("/input/1/type"),
            Some(&json!("function_call_output"))
        );
    }
    #[test]
    fn aggregates_text() {
        let events = vec![SseEvent {
            event: "message".into(),
            data: json!({"type":"response.output_text.delta","delta":"hi"}).to_string(),
        }];
        let out = collect_responses_events(events).unwrap();
        assert_eq!(out.pointer("/output/0/content/0/text"), Some(&json!("hi")));
    }
}
