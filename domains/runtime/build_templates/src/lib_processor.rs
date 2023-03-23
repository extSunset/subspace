use std::collections::HashMap;
use crate::templates::LIB_TEMPLATE as TEMPLATE;
use crate::processor::process_template;

const VARIABLE_DIRECTORY: &str = "src/lib_variables";

pub fn generate() -> Result<String, String> {
    println!("cargo:rerun-if-changed={VARIABLE_DIRECTORY}");
    process_template(TEMPLATE, HashMap::new())
}