#!/bin/sh
# Starts Qwen Code and attaches an interactive session to berkeCouncil.
# Pass any Qwen arguments through, for example: qwen-start.sh --resume
# To use an OpenAI-compatible endpoint, set QWEN_KEY_FILE, QWEN_BASE_URL and QWEN_MODEL.
input=$HOME/.config/council/qwen/input.jsonl
events=$HOME/.config/council/qwen/events.jsonl
mkdir -p "$(dirname "$input")"
chmod 700 "$(dirname "$input")"

if [ -n "${QWEN_KEY_FILE:-}" ]; then
    OPENAI_API_KEY=$(tr -d ' \t\r\n' 2>/dev/null < "$QWEN_KEY_FILE")
    if [ -z "$OPENAI_API_KEY" ]; then
        echo "qwen-start: cannot read QWEN_KEY_FILE; not starting." >&2
        exit 1
    fi
    OPENAI_BASE_URL=${QWEN_BASE_URL:?set QWEN_BASE_URL}
    OPENAI_MODEL=${QWEN_MODEL:?set QWEN_MODEL}
    export OPENAI_API_KEY OPENAI_BASE_URL OPENAI_MODEL
fi

# The channel serves one interactive session at a time.
if ! pgrep -f "^([^ ]*/)?node .*--input-file $input" >/dev/null 2>&1; then
    : > "$input"; : > "$events"; chmod 600 "$input" "$events"
    exec qwen --input-file "$input" --json-file "$events" "$@"
fi
exec qwen "$@"
