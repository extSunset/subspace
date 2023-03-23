use crate::processor::{process_template, read_variables, Variable};
use crate::templates::RUNTIME_TEMPLATE as TEMPLATE;

const VARIABLE_DIRECTORY: &str = "src/runtime_variables";
const SINGLE_VALUE_VARIABLE_JSON_FILE: &str = "singles.json";

pub const VARIABLES: [Variable<'static>; 11] = [
    Variable{
        optional: false,
        single_value: true,
        variable_name: "domain_id_const_param"
    },
    Variable {
        optional: false,
        single_value: true,
        variable_name: "domain_id_ref"
    },
    Variable {
        optional: false,
        single_value: true,
        variable_name: "spec_name"
    },
    Variable {
        optional: false,
        single_value: true,
        variable_name: "impl_name"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "imports"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "pallet_parameters"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "pallet_config"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "runtime_pallet_declaration"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "runtime_api"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "list_benchmarks"
    },
    Variable {
        optional: true,
        single_value: false,
        variable_name: "add_benchmarks"
    }
];

pub fn generate() -> Result<String, String> {
    println!("cargo:rerun-if-changed={VARIABLE_DIRECTORY}");
    let variables = read_variables(VARIABLES.to_vec(), &VARIABLE_DIRECTORY.to_string(), &SINGLE_VALUE_VARIABLE_JSON_FILE.to_string())?;
    process_template(TEMPLATE, variables)
}




