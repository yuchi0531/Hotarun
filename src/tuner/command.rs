//! command 組み立て (SPEC §6)。
//!
//! - `<channel>` は物理ch解決 **後** に代入する。呼び出し側で
//!   [`crate::tuner::channel::resolve_physical_channel`] 済みの値を渡すこと。
//! - 未知の `<...>` は空文字として展開する。
//! - shell 禁止。展開後に shell 風分割して `spawn(program, args)` する。
//! - `tlvDecoder` / `decoder` は変数展開なし・shell 分割のみ (§6)。

/// `<channel>` を物理chに置換し、未知の `<...>` を空文字にする。
pub fn expand_channel_vars(template: &str, physical_channel: &str) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '<' {
            out.push(c);
            continue;
        }
        // `<...>` を拾う。閉じ `>` がなければ残りをそのまま出す。
        let mut name = String::new();
        let mut closed = false;
        for nc in chars.by_ref() {
            if nc == '>' {
                closed = true;
                break;
            }
            name.push(nc);
        }
        if !closed {
            out.push('<');
            out.push_str(&name);
            break;
        }
        if name == "channel" {
            out.push_str(physical_channel);
        } else {
            // 未知変数は空文字 (§6)。
        }
    }
    out
}

/// shell を起動せずに `program args...` へ分割する。
/// single / double quote と `\` エスケープのみ解釈する。
pub fn shell_split(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    let mut has_token = false;
    for c in input.chars() {
        if escaped {
            cur.push(c);
            escaped = false;
            has_token = true;
            continue;
        }
        // single quote 内では `\` を解釈しない。
        if c == '\\' && !in_single {
            escaped = true;
            has_token = true;
            continue;
        }
        if c == '\'' && !in_double {
            in_single = !in_single;
            has_token = true;
            continue;
        }
        if c == '"' && !in_single {
            in_double = !in_double;
            has_token = true;
            continue;
        }
        if c.is_whitespace() && !in_single && !in_double {
            if has_token {
                args.push(std::mem::take(&mut cur));
                has_token = false;
            }
            continue;
        }
        cur.push(c);
        has_token = true;
    }
    if escaped {
        // 末尾の孤立した `\` は文字として扱う。
        cur.push('\\');
    }
    if has_token {
        args.push(cur);
    }
    args
}

/// チューナー command 用: `<channel>` 展開 → 分割 → `(program, args)`。
pub fn build_tuner_command(
    template: &str,
    physical_channel: &str,
) -> Result<(String, Vec<String>), String> {
    if template.trim().is_empty() {
        return Err("empty tuner command".to_owned());
    }
    let expanded = expand_channel_vars(template, physical_channel);
    split_program_args(&expanded)
}

/// `tlvDecoder` / `decoder` 用: 変数展開なし・shell 分割のみ (§6)。
pub fn build_passthrough_command(template: &str) -> Result<(String, Vec<String>), String> {
    if template.trim().is_empty() {
        return Err("empty decoder command".to_owned());
    }
    split_program_args(template)
}

fn split_program_args(expanded: &str) -> Result<(String, Vec<String>), String> {
    let parts = shell_split(expanded);
    if parts.is_empty() {
        return Err("empty tuner command after expansion".to_owned());
    }
    let program = parts[0].clone();
    if program.is_empty() {
        return Err("empty tuner program after expansion".to_owned());
    }
    Ok((program, parts[1..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_is_replaced_after_resolve() {
        let out = expand_channel_vars("recdvb --dev 0 <channel> - -", "13");
        assert_eq!(out, "recdvb --dev 0 13 - -");
    }

    #[test]
    fn unknown_vars_become_empty() {
        assert_eq!(expand_channel_vars("a <foo> b", "13"), "a  b");
        assert_eq!(
            expand_channel_vars("<channel>-<type>-<unknown>", "BS01_0"),
            "BS01_0--"
        );
    }

    #[test]
    fn build_splits_without_shell() {
        let (prog, args) = build_tuner_command("recpt1 --device /dev/pt3video0 <channel> - -", "27")
            .unwrap();
        assert_eq!(prog, "recpt1");
        assert_eq!(args, vec!["--device", "/dev/pt3video0", "27", "-", "-"]);
    }

    #[test]
    fn build_respects_quotes() {
        let (prog, args) = build_tuner_command(r#"prog "a b" '<channel>'"#, "13").unwrap();
        assert_eq!(prog, "prog");
        assert_eq!(args, vec!["a b", "13"]);
    }

    #[test]
    fn passthrough_has_no_expansion() {
        let (prog, args) = build_passthrough_command("tlvdecoder --arg1 <channel>").unwrap();
        assert_eq!(prog, "tlvdecoder");
        assert_eq!(args, vec!["--arg1", "<channel>"]);
    }

    #[test]
    fn empty_command_is_error() {
        assert!(build_tuner_command("   ", "13").is_err());
        assert!(build_tuner_command("<unknown>", "13").is_err());
    }
}
