use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionTriple {
    pub major: usize,
    pub minor: usize,
    pub micro: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub qemu: VersionTriple,
    pub package: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QMPCapability {
    oob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QMPGreeting {
    pub version: VersionInfo,
    pub capabilities: Vec<QMPCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerGreeting {
    pub QMP: QMPGreeting,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn capability_serializes_as_string() {
        assert_eq!(
            serde_json::to_string(&QMPCapability::oob).unwrap(),
            "\"oob\""
        );
    }

    #[test]
    fn greeting_parses_and_round_trips() {
        let raw = json!({
            "QMP": {
                "version": {
                    "qemu": {"major": 9, "minor": 0, "micro": 0},
                    "package": "test-pkg"
                },
                "capabilities": ["oob"]
            }
        });
        let greeting: ServerGreeting = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(greeting.QMP.version.qemu.major, 9);
        assert_eq!(greeting.QMP.version.qemu.minor, 0);
        assert_eq!(greeting.QMP.version.package, "test-pkg");
        assert_eq!(greeting.QMP.capabilities, vec![QMPCapability::oob]);
        assert_eq!(serde_json::to_value(greeting).unwrap(), raw);
    }

    #[test]
    fn greeting_without_capabilities() {
        let greeting: ServerGreeting = serde_json::from_value(json!({
            "QMP": {
                "version": {"qemu": {"major": 9, "minor": 0, "micro": 0}, "package": ""},
                "capabilities": []
            }
        }))
        .unwrap();
        assert!(greeting.QMP.capabilities.is_empty());
    }
}
