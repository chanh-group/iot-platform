use std::fs;
use std::path::Path;

pub fn validate_bundle(dist: &Path) -> Result<(), String> {
    let index_path = dist.join("index.html");
    let index = fs::read_to_string(&index_path)
        .map_err(|error| format!("frontend index missing or unreadable: {error}"))?;
    let mut has_local_script = false;

    for raw_tag in index.split('<').skip(1) {
        let Some((tag, _)) = raw_tag.split_once('>') else {
            break;
        };
        let tag = tag.trim_start();
        let name = tag
            .split(|character: char| character.is_ascii_whitespace() || character == '/')
            .next()
            .unwrap_or_default();

        if let Some(source) = attribute(tag, "src") {
            if name.eq_ignore_ascii_case("script") && local_asset_path(source).is_some() {
                has_local_script = true;
            }
            check_asset(dist, source)?;
        }
        if name.eq_ignore_ascii_case("link")
            && let Some(href) = attribute(tag, "href")
        {
            check_asset(dist, href)?;
        }
    }

    if !has_local_script {
        return Err("frontend index has no local <script src> entry".to_owned());
    }

    Ok(())
}

fn attribute<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let mut offset = 0;
    while let Some(found) = tag.get(offset..)?.find(name) {
        let start = offset + found;
        let after_name = start + name.len();
        let at_boundary = start == 0 || tag.as_bytes()[start - 1].is_ascii_whitespace();
        if at_boundary
            && let Some(value) = tag
                .get(after_name..)
                .map(str::trim_start)
                .and_then(|value| value.strip_prefix('='))
                .map(str::trim_start)
            && let Some(quote) = value.as_bytes().first().copied()
            && matches!(quote, b'\'' | b'"')
            && let Some(contents) = value.get(1..)
            && let Some(end) = contents.as_bytes().iter().position(|byte| *byte == quote)
        {
            return contents.get(..end);
        }
        offset = after_name;
    }
    None
}

fn check_asset(dist: &Path, reference: &str) -> Result<(), String> {
    let Some(path) = local_asset_path(reference) else {
        return Ok(());
    };
    if path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(format!(
            "frontend index contains an unsafe asset path: {reference}"
        ));
    }

    if !dist.join(path).is_file() {
        return Err(format!(
            "frontend index references missing asset: {reference}"
        ));
    }
    Ok(())
}

fn local_asset_path(reference: &str) -> Option<&str> {
    if reference.is_empty() || reference.starts_with('#') {
        return None;
    }

    let path = reference.split(['?', '#']).next().unwrap_or_default();
    if path.starts_with("//") || path.contains(':') {
        return None;
    }
    let path = path.strip_prefix('/').unwrap_or(path);
    (!path.is_empty()).then_some(path)
}
