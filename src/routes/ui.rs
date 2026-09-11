use std::sync::Arc;

use axum::{
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

use crate::config::AppState;

const INDEX: &str = r##"<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Hotarun</title><link rel="stylesheet" href="/ui/style.css"></head><body><header><h1>Hotarun</h1><nav><a href="/ui/">Dashboard</a><a href="/ui/tuners">Tuners</a><a href="/ui/channels">Channels</a><a href="/ui/scan">Scan</a><a href="/ui/logs">Logs</a><a href="/ui/configuration">Configuration</a></nav></header><main id="app"><p>Loading…</p></main><script src="/ui/app.js"></script></body></html>"##;
const STYLE: &str = r##"body{font:16px system-ui,sans-serif;margin:0;background:#10151b;color:#e8eef4}header{padding:1rem 6vw;border-bottom:1px solid #2c3945;display:flex;gap:2rem;align-items:center;flex-wrap:wrap}h1{margin:0;color:#8de0c0}nav{display:flex;gap:1rem;flex-wrap:wrap}a{color:#9fd5ff}main{padding:2rem 6vw}table{border-collapse:collapse;width:100%;background:#17202a}th,td{padding:.65rem;text-align:left;border-bottom:1px solid #33414d}button{padding:.6rem 1rem;background:#2b9d78;color:white;border:0;border-radius:4px}pre{background:#17202a;padding:1rem;overflow:auto}"##;
const SCRIPT: &str = r##"
const app = document.querySelector('#app');
const path = location.pathname;

async function get(u) {
    const r = await fetch(u);
    if (!r.ok) throw Error(await r.text());
    return r.json();
}

function clear() {
    while (app.firstChild) app.removeChild(app.firstChild);
}

function text(tag, value) {
    const node = document.createElement(tag);
    node.textContent = String(value ?? '');
    return node;
}

function table(rows) {
    if (!rows.length) return text('p', 'No entries.');
    const table = document.createElement('table');
    const keys = Object.keys(rows[0]);
    const head = document.createElement('tr');
    keys.forEach(k => head.appendChild(text('th', k)));
    table.createTHead().appendChild(head);
    const body = table.createTBody();
    rows.forEach(row => {
        const tr = document.createElement('tr');
        keys.forEach(k => {
            const value = row[k];
            tr.appendChild(text('td', typeof value === 'object' ? JSON.stringify(value) : value));
        });
        body.appendChild(tr);
    });
    return table;
}

function button(label, handler) {
    const b = text('button', label);
    b.type = 'button';
    b.addEventListener('click', handler);
    return b;
}

function logTable(entries) {
    if (!entries.length) return text('p', 'No log entries.');
    const table = document.createElement('table');
    const head = document.createElement('tr');
    ['timestamp', 'level', 'message'].forEach(k => head.appendChild(text('th', k)));
    table.createTHead().appendChild(head);
    const body = table.createTBody();
    entries.forEach(entry => {
        const row = document.createElement('tr');
        ['timestamp', 'level', 'message'].forEach(field => {
            row.appendChild(text('td', entry && entry[field]));
        });
        body.appendChild(row);
    });
    return table;
}

async function renderLogs() {
    app.append(text('h2', 'Logs'));
    const status = text('p', '');
    const output = document.createElement('div');

    async function loadLogs() {
        refresh.disabled = true;
        status.textContent = 'Loading logs…';
        output.replaceChildren();
        try {
            const value = await get('/api/log');
            const entries = Array.isArray(value.entries) ? value.entries : [];
            output.append(logTable(entries));
            status.textContent = entries.length ? `${entries.length} log entries.` : 'No log entries.';
        } catch (error) {
            status.textContent = 'Failed to load logs: ' + String(error);
        } finally {
            refresh.disabled = false;
        }
    }

    const refresh = button('Refresh', loadLogs);
    app.append(refresh, status, output);
    await loadLogs();
}

function restartControl() {
    const status = text('p', '');
    const restart = button('Restart', async () => {
        if (!confirm('Restart Hotarun?')) return;
        restart.disabled = true;
        status.textContent = 'Requesting restart…';
        try {
            const response = await fetch('/api/config/restart', {
                method: 'POST',
            });
            if (!response.ok) {
                const detail = await response.text();
                status.textContent = 'Restart failed: ' + (detail || response.statusText);
                restart.disabled = false;
                return;
            }
            status.textContent = 'Restart requested. The connection may close while Hotarun restarts.';
        } catch (error) {
            status.textContent = 'Restart may have been requested, but the connection closed before confirmation. Check Hotarun status.';
        }
    });
    return [restart, status];
}

async function render() {
    try {
        clear();
        if (path.includes('tuners')) {
            app.append(text('h2', 'Tuners'), table(await get('/api/tuners')));
        } else if (path.includes('channels')) {
            app.append(text('h2', 'Channels'), table(await get('/api/channels')));
        } else if (path.includes('scan')) {
            app.append(text('h2', 'Scan'));
            const form = document.createElement('form');
            const type = document.createElement('select');
            ['', 'GR', 'BS', 'CS', 'SKY', 'BS4K'].forEach(v => {
                const o = text('option', v || 'All');
                o.value = v;
                type.appendChild(o);
            });
            const dry = document.createElement('input');
            dry.type = 'checkbox';
            const refresh = document.createElement('input');
            refresh.type = 'checkbox';
            refresh.checked = false;
            const status = document.createElement('pre');
            form.append(
                text('label', 'Type '),
                type,
                text('label', ' Dry run '),
                dry,
                text('label', ' Refresh '),
                refresh,
                button('Start', async () => {
                    const q = new URLSearchParams({
                        async: 'true',
                        dryRun: String(dry.checked),
                        refresh: String(refresh.checked),
                    });
                    if (type.value) q.set('type', type.value);
                    await fetch('/api/config/channels/scan?' + q, { method: 'PUT' });
                    await poll();
                }),
                button('Cancel', async () => {
                    await fetch('/api/config/channels/scan', { method: 'DELETE' });
                    await poll();
                }),
            );
            app.append(form, status);
            const poll = async () => {
                const value = await get('/api/config/channels/scan');
                status.textContent = JSON.stringify(value, null, 2);
            };
            await poll();
            setInterval(poll, 1000);
        } else if (path.includes('logs')) {
            await renderLogs();
        } else if (path.includes('configuration')) {
            app.append(text('h2', 'Configuration'));
            for (const name of ['channels', 'tuners', 'server']) {
                const area = document.createElement('textarea');
                area.rows = 12;
                area.cols = 80;
                area.value = JSON.stringify(await get('/api/config/' + name), null, 2);
                const save = button('Save ' + name, async () => {
                    let value;
                    try {
                        value = JSON.parse(area.value);
                    } catch (e) {
                        alert('Invalid JSON');
                        return;
                    }
                    const r = await fetch('/api/config/' + name, {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(value),
                    });
                    if (!r.ok) alert(await r.text());
                });
                app.append(text('h3', name), area, save);
            }
        } else {
            app.append(text('h2', 'Dashboard'));
            const pre = text('pre', JSON.stringify(await get('/api/status'), null, 2));
            const restart = restartControl();
            app.append(text('h3', 'Service'), restart[0], restart[1], pre);
        }
    } catch (e) {
        clear();
        app.append(text('p', e));
    }
}

render().catch(error => {
    clear();
    app.append(text('p', error));
});
"##;

async fn index(State(_state): State<Arc<AppState>>) -> Html<&'static str> {
    Html(INDEX)
}

async fn style(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/css")], STYLE)
}

async fn script(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        SCRIPT,
    )
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(index))
        .route("/ui", get(index))
        .route("/ui/", get(index))
        .route("/ui/style.css", get(style))
        .route("/ui/app.js", get(script))
        .route("/ui/{*path}", get(index))
}

#[cfg(test)]
mod tests {
    use super::{INDEX, SCRIPT};

    #[test]
    fn ui_initial_html_loads_script_and_script_starts_rendering() {
        assert!(INDEX.contains("<main id=\"app\"><p>Loading…</p></main>"));
        assert!(INDEX.contains("<script src=\"/ui/app.js\"></script>"));
        assert!(SCRIPT.contains("async function render()"));
        assert!(SCRIPT.contains("render().catch("));
    }

    #[test]
    fn ui_renders_values_as_text_and_exposes_real_scan_and_save_actions() {
        assert!(!SCRIPT.contains("innerHTML"));
        assert!(SCRIPT.contains("textContent"));
        assert!(SCRIPT.contains("method: 'PUT'"));
        assert!(SCRIPT.contains("method: 'DELETE'"));
        assert!(SCRIPT.contains("b.type = 'button'"));
        assert!(SCRIPT.contains("'SKY'"));
        assert!(SCRIPT.contains("dryRun"));
        assert!(SCRIPT.contains("refresh"));
    }

    #[test]
    fn ui_exposes_logs_and_restart_actions_without_html_injection() {
        assert!(INDEX.contains("href=\"/ui/logs\">Logs</a>"));
        assert!(SCRIPT.contains("path.includes('logs')"));
        assert!(SCRIPT.contains("get('/api/log')"));
        assert!(SCRIPT.contains("timestamp"));
        assert!(SCRIPT.contains("level"));
        assert!(SCRIPT.contains("message"));
        assert!(SCRIPT.contains("text('td', entry && entry[field])"));
        assert!(SCRIPT.contains("/api/config/restart"));
        assert!(SCRIPT.contains("method: 'POST'"));
        assert!(SCRIPT.contains("confirm('Restart Hotarun?')"));
        assert!(SCRIPT.contains("restart.disabled = true"));
        assert!(!SCRIPT.contains("entries.map(entry => `<"));
    }
}
