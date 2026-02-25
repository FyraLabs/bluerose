use serde_json::Value as JsonValue;
use std::collections::HashMap;
use zbus::zvariant::{Optional, OwnedValue, Type, Value};

typify::import_types!(schema = "progress-v0.schema.json", derives = [Clone, Debug],);


/// D-Bus compatible representation of the Event struct, which can be converted from the original Event struct emitted by bootc.
/// 
// The conversion is kinda hacky though, but I wanted it to be stable so we can just
// update schemas and nothing else in code would have to be changed
#[derive(Clone, Debug, Type, Value, OwnedValue)]
pub struct DbusEvent {
    pub kind: String,
    pub fields: HashMap<String, OwnedValue>,
}

#[derive(Debug)]
pub enum DbusEventError {
    Json(serde_json::Error),
    Zvariant(zbus::zvariant::Error),
}

impl From<serde_json::Error> for DbusEventError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<zbus::zvariant::Error> for DbusEventError {
    fn from(err: zbus::zvariant::Error) -> Self {
        Self::Zvariant(err)
    }
}

fn json_value_to_owned_value(value: JsonValue) -> Result<OwnedValue, DbusEventError> {
    match value {
        JsonValue::Null => Ok(OwnedValue::from(Optional::<u8>::from(None))),
        JsonValue::Bool(b) => Ok(OwnedValue::from(b)),
        JsonValue::Number(n) => {
            if let Some(u) = n.as_u64() {
                Ok(OwnedValue::from(u))
            } else if let Some(i) = n.as_i64() {
                Ok(OwnedValue::from(i))
            } else if let Some(f) = n.as_f64() {
                Ok(OwnedValue::from(f))
            } else {
                Ok(OwnedValue::from(0i64))
            }
        }
        JsonValue::String(s) => Ok(OwnedValue::try_from(Value::from(s))?),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let json = serde_json::to_string(&value)?;
            Ok(OwnedValue::try_from(Value::from(json))?)
        }
    }
}

fn json_object_to_fields(
    obj: serde_json::Map<String, JsonValue>,
) -> Result<HashMap<String, OwnedValue>, DbusEventError> {
    let mut fields = HashMap::new();
    for (key, value) in obj {
        fields.insert(key, json_value_to_owned_value(value)?);
    }
    Ok(fields)
}

impl TryFrom<Event> for DbusEvent {
    type Error = DbusEventError;

    fn try_from(event: Event) -> Result<Self, Self::Error> {
        // hack: convert back to JSON to extract types, then
        // we enumerate each field recursively to convert it into zvariant::OwnedValue.
        // 
        // It's not efficient, but it's flexible and I don't want to write
        // an exhaustive match statement for every single field in the schema, atp
        // i might as well re-implement the schema manually
        let json = serde_json::to_value(&event)?;
        let kind = match &json {
            JsonValue::Object(obj) => obj
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or("Unknown")
                .to_string(),
            _ => "Unknown".to_string(),
        };

        let fields = match json {
            JsonValue::Object(mut obj) => {
                obj.remove("type");
                json_object_to_fields(obj)?
            }
            other => {
                let mut fields = HashMap::new();
                fields.insert("value".to_string(), json_value_to_owned_value(other)?);
                fields
            }
        };

        Ok(Self { kind, fields })
    }
}
