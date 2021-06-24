//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{parse_macro_input, parse_quote, Data, DeriveInput, Fields, GenericParam, Generics};

#[proc_macro_derive(Label)]
pub fn derive_label(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);

    // Used in the quasi-quotation below as `#name`.
    let name = input.ident;

    // Add a bound `T: Label` to every type parameter T.
    let generics = add_trait_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Generate an expression to get the key value pairs of the struct field.
    let expr = key_value_pairs(&input.data);
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics pravega_client::index::Label for #name #ty_generics #where_clause {
            fn to_key_value_pairs(&self) -> Vec<(&'static str, u64)> {
                vec!{#expr}
            }
        }
    };

    // Hand the output tokens back to the compiler.
    proc_macro::TokenStream::from(expanded)
}

// Add a bound `T: Label` to every type parameter T.
fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(Label));
        }
    }
    generics
}

fn key_value_pairs(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => fields
                .named
                .iter()
                .map(|f| {
                    let name = f.ident.as_ref().unwrap();
                    let name_str = format!("{}", name);

                    quote_spanned! {f.span()=>
                        (#name_str, pravega_client::index::Value::value(&self.#name)),
                    }
                })
                .collect(),
            Fields::Unnamed(ref _fields) => {
                quote! {
                    compile_error!("expected named fields");
                }
            }
            Fields::Unit => {
                quote! {
                    compile_error!("expected named fields");
                }
            }
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}
