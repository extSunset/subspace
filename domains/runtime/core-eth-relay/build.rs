#[cfg(feature = "std")]
use std::fs;
#[cfg(feature = "std")]
use std::io::Write;

fn main() {
    #[cfg(feature = "std")]
    {
        // Read templates
        let mut handle = fs::File::create("src/runtime.rs").unwrap();
        let generated_string = build_templates::generate_runtime().unwrap();
        handle.write(generated_string.as_bytes()).unwrap();


        let mut handle = fs::File::create("src/lib.rs").unwrap();
        let generated_string = build_templates::generate_lib().unwrap();
        handle.write(generated_string.as_bytes()).unwrap();

        substrate_wasm_builder::WasmBuilder::new()
            .with_current_project()
            .enable_feature("wasm-builder")
            .export_heap_base()
            .import_memory()
            .build();
    }

    subspace_wasm_tools::export_wasm_bundle_path();
}
