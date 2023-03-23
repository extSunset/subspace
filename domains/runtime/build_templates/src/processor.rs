use std::collections::HashMap;
use std::fs;
use serde_json::{Value, Map};

#[derive(Clone)]
pub struct Variable<'a> {
    pub optional: bool,
    pub single_value: bool,
    pub variable_name: &'a str,
}

fn read_single_value_map(variable_directory: &String, single_value_file: &String) -> Result<Map<String, Value>, String> {
    let single_value_file = format!("{variable_directory}/{single_value_file}");
    let single_values = fs::read_to_string(&single_value_file).map_err(|e| format!("unable to open file: {} error is: {}", &single_value_file, e))?;
    let parsed: Value = serde_json::from_str(&single_values).map_err(|e| format!("{e}"))?;
    let maybe_value: Option<&Map<String, Value>> = parsed.as_object();
    if let Some(value) = maybe_value {
        Ok(value.clone())
    } else {
        Err(format!("unable to read single value from {}/{}", variable_directory, &single_value_file))
    }
}

pub fn read_variables(variable_infos: Vec<Variable>, variable_directory: &String, single_value_file: &String) -> Result<HashMap<String, String>, String> {
    let mut variables = HashMap::new();
    let single_value_map = read_single_value_map(variable_directory, single_value_file)?;

    for variable_info in variable_infos {
        if variable_info.single_value {
            let maybe_value = single_value_map.get(variable_info.variable_name);
            if !variable_info.optional && (maybe_value.is_none() || !maybe_value.expect("value cannot be none").is_string()) {
                return Err(format!("Required single value variable: {} is either absent or not of string type", variable_info.variable_name));
            }
            if let Some(value) = maybe_value {
                if !value.is_string() {
                    return Err(format!("Single value variable: {} is not of string type", variable_info.variable_name));
                }
                variables.insert(variable_info.variable_name.to_string(), value.as_str().expect("already checked for string type earlier").to_string());
            } else {
                variables.insert(variable_info.variable_name.to_string(), "".to_string());
            }
        } else {
            let multi_value_file = format!("{}/{}.value", variable_directory, variable_info.variable_name);
            let maybe_value = fs::read_to_string(&multi_value_file);
            if !variable_info.optional && maybe_value.is_err() {
                return Err(format!("Unable to read required multi value variable: {} from filename: {} with error: {}", variable_info.variable_name, &multi_value_file, maybe_value.expect_err("we already checked for error condition")));
            }

            if let Ok(value) = maybe_value {
                variables.insert(variable_info.variable_name.to_string(), value.to_string());
            } else {
                variables.insert(variable_info.variable_name.to_string(), "".to_string());
            }
        }
    }

    Ok(variables)
}

pub fn process_template(template: &str, values: HashMap<String, String>) -> Result<String, String> {
    let mut processed = String::from(template);

    for (variable, value) in values {
        processed = str::replace(processed.as_str(), format!("@@{variable}@@").as_str(), &value);
    }

    Ok(processed)
}