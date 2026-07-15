use std::collections::BTreeMap;

use agent_client_protocol::schema::v1::{Usage, UsageUpdate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Thor,
    Loki,
    Eitri,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Code,
    Explore,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub role: Role,
    pub purpose: Option<Purpose>,
    pub usage: Option<Usage>,
    pub update: Option<UsageUpdate>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RoleUsage {
    pub prompts: u64,
    pub total_tokens: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub thought_tokens: u64,
    pub context_used: u64,
    pub context_size: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub costs: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub thor: RoleUsage,
    pub loki: RoleUsage,
    pub eitri_code: RoleUsage,
    pub eitri_explore: RoleUsage,
}

impl Snapshot {
    pub fn observe(&mut self, record: Record) {
        let usage = match (record.role, record.purpose) {
            (Role::Thor, _) => &mut self.thor,
            (Role::Loki, _) => &mut self.loki,
            (Role::Eitri, Some(Purpose::Explore)) => &mut self.eitri_explore,
            (Role::Eitri, _) => &mut self.eitri_code,
        };
        usage.prompts += 1;
        if let Some(value) = record.usage {
            usage.total_tokens += value.total_tokens;
            usage.input_tokens += value.input_tokens;
            usage.output_tokens += value.output_tokens;
            usage.thought_tokens += value.thought_tokens.unwrap_or_default();
        }
        if let Some(update) = record.update {
            usage.context_used = update.used;
            usage.context_size = update.size;
            if let Some(cost) = update.cost {
                *usage.costs.entry(cost.currency).or_default() += cost.amount;
            }
        }
    }

    pub fn eitri(&self) -> RoleUsage {
        let mut total = self.eitri_code.clone();
        total.prompts += self.eitri_explore.prompts;
        total.total_tokens += self.eitri_explore.total_tokens;
        total.input_tokens += self.eitri_explore.input_tokens;
        total.output_tokens += self.eitri_explore.output_tokens;
        total.thought_tokens += self.eitri_explore.thought_tokens;
        total.context_used += self.eitri_explore.context_used;
        total.context_size += self.eitri_explore.context_size;
        for (currency, amount) in &self.eitri_explore.costs {
            *total.costs.entry(currency.clone()).or_default() += amount;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eitri_code_and_explore_are_separate_and_aggregate() {
        let mut snapshot = Snapshot::default();
        snapshot.observe(Record {
            role: Role::Eitri,
            purpose: Some(Purpose::Code),
            usage: Some(Usage::new(10, 7, 3)),
            update: None,
        });
        snapshot.observe(Record {
            role: Role::Eitri,
            purpose: Some(Purpose::Explore),
            usage: Some(Usage::new(20, 15, 5)),
            update: None,
        });

        assert_eq!(snapshot.eitri_code.total_tokens, 10);
        assert_eq!(snapshot.eitri_explore.total_tokens, 20);
        assert_eq!(snapshot.eitri().total_tokens, 30);
    }
}
