if [ -z "$PYRSS_PYTHON_PATH" ]
then
    export PYRSS_PYTHON_PATH=$PYRSS_DIR/
    export PYTHONPATH=$PYRSS_PYTHON_PATH:$PYTHONPATH
fi


alias pyrss="python3 ${PYRSS_DIR}/pyrss.py"

pyrss-db-size() {
    local db_path="${1:-${PYRSS_DB_PATH:-$HOME/.local/share/pyrss/rss.sqlite3}}"

    if [ ! -e "$db_path" ]; then
        echo "Database not found at $db_path"
        return 1
    fi

    du -sh "$db_path"
}

pyrss-open-entry() {
    local feed_query="$1"
    local feed_line=""

    if [ -n "$feed_query" ]; then
        feed_line=$(pyrss list | rg -i --fixed-strings -- "$feed_query" | default-fuzzy-finder)
    else
        feed_line=$(pyrss list | default-fuzzy-finder)
    fi

    if [ -z "$feed_line" ]; then
        return 0
    fi

    local feed_id=""
    feed_id=$(printf "%s" "$feed_line" | awk '{print $1}')
    if [ -z "$feed_id" ]; then
        echo "Could not determine feed ID."
        return 1
    fi

    local entry_line=""
    entry_line=$(pyrss recent "$feed_id" --limit 50 | default-fuzzy-finder)
    if [ -z "$entry_line" ]; then
        return 0
    fi

    local url=""
    url=$(printf "%s" "$entry_line" | awk -F '\t' '{print $3}')
    if [ -z "$url" ]; then
        echo "No link available for the selected entry."
        return 1
    fi

    open "$url"
}
