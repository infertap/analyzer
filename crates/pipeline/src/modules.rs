// SPDX-License-Identifier: AGPL-3.0-only

//! Build-time module selection belongs to the execution layer. Pure derivation types
//! remain in `analysis`; deployment configuration and execution registration live here.

use crate::adapters::{Registration, CORE, OFFERED};

/// The public modules this build registers: none. An application appends its static modules
/// before validation; the core registers are the keyed driver's own operators.
#[must_use]
pub fn registrations() -> Vec<Registration> {
    Vec::new()
}
use analyzer_analysis::module::fold_order;
use analyzer_analysis::Error;

/// The public modules one deployment runs: the registered less what its configuration
/// disables, checked against their needs (a module another enabled module needs cannot be
/// switched off alone) and ordered for each settled day. The core registers are always on.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModuleSet {
    order: Vec<&'static str>,
    registrations: Vec<Registration>,
}

impl ModuleSet {
    /// Every registered module, in fold order.
    ///
    /// # Panics
    /// If the registry's own needs do not order — a build defect, pinned below.
    #[must_use]
    pub fn all() -> Self {
        Self::without(&[]).expect("the registry orders")
    }

    /// The registry less `disabled`, by name.
    ///
    /// # Errors
    /// A name no module declares; a core register; a disabled module that an enabled one
    /// needs; a cycle.
    pub fn without(disabled: &[&str]) -> Result<Self, Error> {
        Self::from_registrations(&registrations(), disabled)
    }

    /// Validate supplied registrations, including disabled definitions, before execution.
    ///
    /// # Errors
    /// Invalid or duplicate names, missing/cyclic dependencies, unsupported initialization
    /// (including `ExaminedHistory` at window cadence), incompatible cadence,
    /// or disabling a prerequisite of an enabled module.
    pub fn from_registrations(
        registrations: &[Registration],
        disabled: &[&str],
    ) -> Result<Self, Error> {
        let registry: Vec<_> = registrations.iter().map(|r| r.spec).collect();
        for r in registrations {
            let name = r.spec.name;
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                || !name.as_bytes()[0].is_ascii_lowercase()
            {
                return Err(Error::Modules(format!("invalid module name {name:?}")));
            }
            if CORE.contains(&name) {
                return Err(Error::Modules(format!(
                    "{name} is a core register, which no module may be named"
                )));
            }
        }
        // The core register a module may depend on is always there, first.
        let offered: &'static [&'static str] = &[];
        let graph = |m: &analyzer_analysis::module::ModuleSpec| (m.name, m.needs);
        fold_order(
            &std::iter::once((OFFERED, offered))
                .chain(registry.iter().map(graph))
                .collect::<Vec<_>>(),
        )?;
        let mut names = std::collections::BTreeSet::new();
        for registration in registrations {
            registration.validate_initialization()?;
            if !names.insert(registration.spec.name) {
                return Err(Error::Modules(format!(
                    "duplicate module {}",
                    registration.spec.name
                )));
            }
            for dependency in registration.spec.needs {
                if let Some(required) = registrations.iter().find(|r| r.spec.name == *dependency) {
                    if !registration.can_depend_on(required) {
                        return Err(Error::Modules(format!(
                            "{} requires {dependency} in a different cadence",
                            registration.spec.name
                        )));
                    }
                }
            }
        }
        for name in disabled {
            if CORE.contains(name) {
                return Err(Error::Modules(format!(
                    "{name} is a core register and always runs; only public modules can be \
                     disabled"
                )));
            }
            if !registry.iter().any(|m| m.name == *name) {
                let known: Vec<&str> = registry.iter().map(|m| m.name).collect();
                return Err(Error::Modules(format!(
                    "no module is named {name:?}; this build has {}",
                    known.join(", ")
                )));
            }
        }
        let enabled: Vec<(&'static str, &'static [&'static str])> = registry
            .iter()
            .filter(|m| !disabled.contains(&m.name))
            .map(|m| (m.name, m.needs))
            .collect();
        for (name, needs) in &enabled {
            if let Some(off) = needs.iter().find(|n| disabled.contains(n)) {
                return Err(Error::Modules(format!(
                    "module {name} needs {off}, which is disabled; disable {name} too, or \
                     enable {off}"
                )));
            }
        }
        let order: Vec<&'static str> = fold_order(
            &std::iter::once((OFFERED, offered))
                .chain(enabled.iter().map(|(n, needs)| (*n, *needs)))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .filter(|n| *n != OFFERED)
        .collect();
        let mut selected: Vec<_> = order
            .iter()
            .map(|name| {
                registrations
                    .iter()
                    .find(|r| r.spec.name == *name)
                    .cloned()
                    .ok_or_else(|| Error::Modules(format!("missing registration {name}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        selected.extend(
            registrations
                .iter()
                .filter(|r| disabled.contains(&r.spec.name))
                .cloned(),
        );
        Ok(Self {
            order,
            registrations: selected,
        })
    }

    /// Every registration, the disabled included.
    pub(crate) fn registered(&self) -> impl Iterator<Item = &Registration> {
        self.registrations.iter()
    }

    /// The enabled registrations, in fold order.
    pub(crate) fn enabled(&self) -> impl Iterator<Item = &Registration> {
        self.order
            .iter()
            .filter_map(|name| self.registrations.iter().find(|r| r.spec.name == *name))
    }

    /// Whether the named module runs.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        self.order.contains(&name)
    }

    /// The enabled modules in fold order.
    #[must_use]
    pub fn order(&self) -> &[&'static str] {
        &self.order
    }
}

#[cfg(test)]
mod tests {
    use super::ModuleSet;

    #[test]
    fn a_core_register_is_always_on_and_no_module_takes_its_name() {
        assert!(
            ModuleSet::all().order().is_empty(),
            "this build registers no module"
        );
        for core in crate::adapters::CORE {
            let refused = ModuleSet::without(&[core]).expect_err("a core register");
            assert!(refused.to_string().contains("core register"), "{refused}");
        }
        let unknown = ModuleSet::without(&["residency"]).expect_err("not a module");
        assert!(
            unknown.to_string().contains("no module is named"),
            "{unknown}"
        );
    }
}
