use std::io::Result;
fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config
        .type_attribute(
            ".",
            "#[derive(serde::Serialize,serde::Deserialize,std::cmp::Eq)]",
        )
        .compile_protos(&["src/reqres.proto"], &["src/"])?;
    Ok(())
}
