# berkeCouncil

One local web page for talking to several AI coding agents at once. Ask one question, read each agent's answer in its own column, and optionally have one of them merge the answers into a summary.

- **Live sessions.** Messages go into the agent sessions already open in your terminals, with their history: Claude Code, Codex, Grok and Qwen Code.
- **Guests.** Any model behind an OpenAI-compatible API can answer in its own column.
- **Local.** It binds to `127.0.0.1` and is a single Python file using only the standard library.
- **Your approvals stay yours.** The page never answers a tool approval. It only tells you when one is waiting in a terminal.
- English and Turkish, chosen from your browser's language.

## Run

Python 3.10 or newer. Install only the agents you want to use.

```sh
python3 -B council_page.py
```

Open <http://127.0.0.1:8790>.

## Configure

The first run creates `~/.config/council/page.json` and a private access token. See `config.example.json` for every field.

| Field | Meaning |
|---|---|
| `grok_session` | Id of the Grok session to talk to |
| `codex_thread`, `codex_rollout` | Id of the Codex thread and the path of its rollout file |
| `claude_url` | Address of the Claude channel |
| `keys_dir` | Folder holding guest API keys, one `<key>.txt` per provider |
| `save_root`, `save_folders` | Where Export may write. Export is off until `save_root` is set |
| `guests` | Replaces the built-in guest model table |

Keep this folder private: it holds your tokens and session ids.

## Connect the agents

- **Grok:** start it with its leader socket enabled, `grok --leader --resume`, and set `grok_session`.
- **Codex:** run it normally so its background service is available, and set `codex_thread` and `codex_rollout`.
- **Claude Code:** register `channel/council_channel.py` as a user-level MCP server named `council`, then start Claude Code with `--dangerously-load-development-channels server:council`. Channels are a research preview.
- **Qwen Code:** start it with `scripts/qwen-start.sh`. Add `--resume` to continue an earlier chat.

The page shows which agents it can reach and refreshes this by itself.

## Security

- The server accepts only `127.0.0.1`, checks `Host` and `Origin`, and requires a token on its API. The page embeds that token, so any program running under your own user can obtain it.
- Answers from other models are treated as untrusted text and are rendered with HTML escaped.
- Export writes only under `save_root`, only into the configured folders, and only when you click it.
- Guest questions are sent to the provider you selected. Do not send anything you would not share with that provider.

## License

Copyright (c) 2026-present Berke GÜLMEN. berkeCouncil is offered under your choice of:

1. **[PolyForm Noncommercial 1.0.0](LICENSE-NONCOMMERCIAL.md)**: free for personal use, study, research and hobby projects, and for charities, schools, public research bodies and government institutions.
2. **[PolyForm Internal Use 1.0.0](LICENSE-INTERNAL-USE.md)**: free for the internal operations of your company.
3. **[Commercial license](LICENSE-COMMERCIAL.txt)**: needed to sell berkeCouncil or to include it in a product or a service. Contact: berkegulmen at outlook dot com
