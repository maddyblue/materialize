#![crate_type = "proc-macro"]
extern crate proc_macro;

use std::collections::HashMap;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Field, Fields, FieldsNamed, FieldsUnnamed, Ident, Item, Type};

fn is_bool(field: &Field) -> bool {
    matches!(
        &field.ty,
        Type::Path(type_path) if type_path.path.is_ident("bool"),
    )
}

fn is_vec(field: &Field) -> bool {
    if let Type::Path(type_path) = &field.ty {
        let segments = &type_path.path.segments;
        return segments[0].ident == "Vec";
    }
    false
}

fn is_option(field: &Field) -> bool {
    if let Type::Path(type_path) = &field.ty {
        let segments = &type_path.path.segments;
        return segments[0].ident == "Option";
    }
    false
}

fn truncate_stmt_suffix(s: &str) -> &str {
    s.trim_end_matches("sStatement")
        .trim_end_matches("Statement")
}

fn split_upper<'a>(mut s: &'a str) -> Vec<&'a str> {
    let mut indexes = Vec::new();
    for (i, c) in s.chars().enumerate().skip(1) {
        if c.is_uppercase() {
            indexes.push(i);
        }
    }
    let mut strs = Vec::with_capacity(indexes.len());
    let mut accum = 0;
    for split_at in indexes {
        let at = split_at - accum;
        let (l, r) = s.split_at(at);
        accum += at;
        s = r;
        strs.push(l);
    }
    strs.push(s);
    strs
}

fn fmt_ident(ident: &Ident) -> String {
    split_upper(truncate_stmt_suffix(&ident.to_string()))
        .join(" ")
        .replace("_", " ")
        .to_uppercase()
}

struct Attrs(HashMap<String, String>);

impl Attrs {
    fn new(attrs: &[Attribute]) -> Self {
        let mut map = HashMap::new();
        for attr in attrs {
            if !attr.path.is_ident("todoc") {
                continue;
            }
            for tok in attr.tokens.clone().into_iter() {
                // TODO: Surely there's a better way to do this.
                if let quote::__private::TokenTree::Group(group) = tok {
                    let mut toks = group
                        .stream()
                        .into_iter()
                        .map(|tok| tok.to_string())
                        .peekable();
                    while toks.peek().is_some() {
                        let name = toks.next().unwrap();
                        let mut value = "".to_string();
                        if let Some("=") = toks.peek().cloned().as_deref() {
                            toks.next();
                            value = toks.next().unwrap();
                            // Trim off the quotes. Gotta be a better way?
                            value = value[1..value.len() - 1].to_string();
                        }
                        map.insert(name, value);
                        match toks.next().as_deref() {
                            Some(",") => continue,
                            None => break,
                            _ => panic!("unexpected attribute token"),
                        }
                    }
                }
            }
        }
        Self(map)
    }

    fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    fn rename(&mut self, name: &str) -> String {
        self.remove("rename").unwrap_or_else(|| name.to_string())
    }

    fn name(&mut self, name: &str) -> Option<String> {
        if self.remove("no_name").is_some() {
            None
        } else {
            Some(self.rename(name))
        }
    }

    fn prefix(&mut self, mut doc: TokenStream2) -> TokenStream2 {
        if let Some(prefix) = self.remove("prefix") {
            doc = quote! { #doc.map(|doc| pretty::RcDoc::text(#prefix).append(doc)) };
        }
        doc
    }

    fn suffix(&mut self, mut doc: TokenStream2) -> TokenStream2 {
        if let Some(suffix) = self.remove("suffix") {
            doc = quote! { #doc.map(|doc| doc.append(pretty::RcDoc::text(#suffix))) };
        }
        doc
    }

    fn nest(&mut self, mut doc: TokenStream2) -> TokenStream2 {
        if let Some(nest) = self.remove("nest") {
            doc = quote! { #doc.map(|doc|
                pretty::RcDoc::text(#nest)
                .append(pretty::RcDoc::line())
                .append(doc)
                .nest(#NEST)
                .group()
            ) };
            if let Some(suffix) = self.remove("nest_suffix") {
                doc = quote! { #doc.map(|doc| doc
                    .append(pretty::RcDoc::line())
                    .append(pretty::RcDoc::text(#suffix))
                    .group()
                ) };
            }
        }
        doc
    }

    fn els(&mut self, mut doc: TokenStream2) -> TokenStream2 {
        if let Some(els) = self.remove("else") {
            doc = quote! { Some(#doc.unwrap_or_else(|| pretty::RcDoc::text(#els))) };
        }
        doc
    }

    fn show_empty(&mut self, mut doc: TokenStream2) -> TokenStream2 {
        if self.remove("show_empty").is_some() {
            doc = quote! { Some(#doc.unwrap_or_else(pretty::RcDoc::nil)) };
        }
        doc
    }

    fn separator(&mut self, default: &str) -> TokenStream2 {
        let sep = self
            .remove("separator")
            .unwrap_or_else(|| default.to_string());
        let mut doc = quote! { pretty::RcDoc::text(#sep) };
        if self.remove("separator_noline").is_none() {
            doc = quote! { #doc.append(pretty::RcDoc::line()) };
        }
        doc
    }
}

impl Drop for Attrs {
    fn drop(&mut self) {
        if !self.0.is_empty() {
            panic!("unknown attributes: {:?}", self.0.keys());
        }
    }
}

const NEST: isize = 4;

#[proc_macro_derive(ToDoc, attributes(todoc))]
pub fn derive_to_doc(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as Item);
    match input {
        Item::Enum(item) => {
            let _enum_attrs = Attrs::new(&item.attrs);
            let variants = item.variants.iter().map(|variant| {
                let ident = &variant.ident;
                let mut variant_attrs = Attrs::new(&variant.attrs);
                let name = variant_attrs
                    .remove("rename")
                    .unwrap_or_else(|| fmt_ident(&variant.ident));
                let FromFields { fields, doc } =
                    from_fields(&variant.fields, Some(name), variant_attrs.separator(""));
                let doc = if variant_attrs.remove("ignore").is_some() {
                    quote! { None }
                } else {
                    let doc = variant_attrs.prefix(doc);
                    let doc = variant_attrs.suffix(doc);
                    let doc = variant_attrs.nest(doc);
                    doc
                };
                quote! { Self::#ident #fields => #doc.unwrap_or_else(pretty::RcDoc::nil), }
            });
            let item_ident = item.ident;
            let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
            quote! {
                impl #impl_generics ToDoc for #item_ident #ty_generics #where_clause {
                    fn to_doc(&self) -> pretty::RcDoc<()> {
                        match self {
                            #(#variants)*
                        }
                    }
                }
            }
            .into()
        }
        Item::Struct(item) => {
            let mut struct_attrs = Attrs::new(&item.attrs);
            let name = fmt_ident(&item.ident);
            let name = struct_attrs.name(&name);
            let FromFields { fields, doc } =
                from_fields(&item.fields, name, struct_attrs.separator(""));
            let item_ident = item.ident;
            let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
            let doc = struct_attrs.suffix(doc);
            quote! {
                impl #impl_generics ToDoc for #item_ident #ty_generics #where_clause {
                    fn to_doc(&self) -> pretty::RcDoc<()> {
                        let Self #fields = self;
                        #doc.unwrap_or_else(pretty::RcDoc::nil).group()
                    }
                }
            }
            .into()
        }
        _ => panic!("unsupported: {:?}", input),
    }
}

type TokenStream2 = proc_macro2::TokenStream;

struct FromField {
    doc: TokenStream2,
    attrs: Attrs,
}

// ident is something like self.blah, name is blah.
fn from_field(field: &Field, ident: &Ident, name: &str) -> FromField {
    let mut attrs = Attrs::new(&field.attrs);
    if let Some(doc_fn) = attrs.remove("doc_fn") {
        let doc_fn = Ident::new(&doc_fn, syn::__private::Span::call_site());
        let doc = quote! { #doc_fn(&self) };
        return FromField { attrs, doc };
    }
    let name = attrs.rename(name);
    let doc = if is_bool(&field) {
        let doc = quote! { #ident.then(|| pretty::RcDoc::text(#name)) };
        let doc = attrs.els(doc);
        doc
    } else if is_vec(&field) {
        let sep = attrs.separator(",");
        let doc = quote! { if #ident.is_empty() {
                None
            } else {
                Some(
                    pretty::RcDoc::intersperse(
                        #ident.iter().map(|v| v.to_doc()),
                        #sep
                    )
                    .group()
                )
            }
        };
        let doc = attrs.name(doc, &name);
        let doc = attrs.show_empty(doc);
        doc
    } else if is_option(&field) {
        let doc = quote! { #ident.as_ref().map(|opt| opt.to_doc()) };
        let doc = attrs.name(doc, &name);
        let doc = attrs.els(doc);
        doc
    } else {
        quote! { Some(#ident.to_doc()) }
    };
    let doc = attrs.prefix(doc);
    let doc = attrs.suffix(doc);
    let doc = attrs.nest(doc);
    FromField { doc, attrs }
}

struct FromFields {
    fields: TokenStream2,
    doc: TokenStream2,
}

fn from_fields(fields: &Fields, name: Option<String>, separator: TokenStream2) -> FromFields {
    match fields {
        Fields::Named(fields) => named_fields(fields, name, separator),
        Fields::Unnamed(fields) => unnamed_fields(fields, name.unwrap()),
        Fields::Unit => FromFields {
            fields: quote! {},
            doc: quote! { Some(pretty::RcDoc::text(#name)) },
        },
    }
}

fn unnamed_fields(fields: &FieldsUnnamed, name: &str) -> FromFields {
    let idents = (0..fields.unnamed.len())
        .map(|i| {
            // TODO: Better way to do this?
            Ident::new(&format!("_{i}"), syn::__private::Span::call_site())
        })
        .collect::<Vec<_>>();
    let doc = match fields.unnamed.len() {
        0 => quote! { Some(pretty::RcDoc::text(#name)) },
        1 => {
            let field = from_field(fields.unnamed.first().unwrap(), &idents[0], name);
            field.doc
        }
        _ => panic!(
            "unsupported: unnamed fields with len {}",
            fields.unnamed.len()
        ),
    };
    let idents = quote! { (#(#idents),*) };
    FromFields {
        fields: idents,
        doc,
    }
}

fn named_fields(fields: &FieldsNamed, name: Option<String>, separator: TokenStream2) -> FromFields {
    let mut ignored = false;
    let (mut docs, mut idents): (Vec<_>, Vec<_>) = fields
        .named
        .iter()
        .filter_map(|field| {
            let ident = field.ident.as_ref().unwrap();
            let FromField { doc, mut attrs } = from_field(field, ident, &fmt_ident(ident));
            if attrs.remove("ignore").is_some() {
                ignored = true;
                None
            } else {
                Some((doc, quote! { #ident }))
            }
        })
        .unzip();
    if let Some(name) = name {
        let name = quote! { pretty::RcDoc::text(#name) };
        docs.insert(0, name);
    }
    let doc = quote! { {
        let docs = [#(#docs),*].into_iter().filter_map(|v| v).collect::<Vec<_>>();
        if docs.is_empty() {
            None
        } else {
            Some(pretty::RcDoc::intersperse(docs, #separator).group())
        }
    } };
    if ignored {
        idents.push(quote! { .. });
    }
    let idents = quote! { {#(#idents),*} };
    FromFields {
        fields: idents,
        doc,
    }
}

/*

For a struct with {..} fields: apply the rules for {..}
For a struct with () fields:
For a enum: current variant converted to doc

Converting a Field to a doc:

() [empty variant or tuple or struct()]: enum variant name or struct field name
bool: field name if true, nil if false
struct/enum: recursive call
{..}: for each field in order, convert to doc, then intersperse with line
Option<T>: nil if None, otherwise field name nested with T converted to doc
Vec<T>: nil if empty, otherwise field name nested with values converted to docs, interspersed with comma line.

*/
