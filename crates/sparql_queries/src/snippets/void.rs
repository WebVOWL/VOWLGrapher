/// The void snippet (returns nothing).
pub const VOID: &str = r#"
            # The void snippet (returns nothing).
            BIND(<http://example.org/nothing> AS ?id)
            BIND(<http://example.org/nothing> AS ?nodeType)
            BIND(<http://example.org/nothing> AS ?target)
            BIND("" AS ?label)
            FILTER(false)
        "#;
