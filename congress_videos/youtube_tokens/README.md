# YouTube OAuth tokens

Per-channel, per-purpose OAuth tokens, stored as portable JSON
(`Credentials.to_json()`) so they load across google-auth versions — unlike
pickle, which breaks when writer and reader run different google-auth majors.
**Tokens are secrets and are never committed** (see `.gitignore`); only this
scaffold is tracked.

## Layout

```
youtube_tokens/
  {channel}/
    upload.json       # scopes: youtube.upload + youtube + youtube.force-ssl
    analytics.json    # scope:  yt-analytics.readonly  (read-only)
```

Channels and the scopes for each purpose are declared in
`congress_videos/config/youtube_channels.py`.

## Generate a token

Run locally (opens a browser for OAuth consent):

```bash
uv run python congress_videos/scripts/generate_youtube_token.py --channel congreso-es-tv --purpose upload
uv run python congress_videos/scripts/generate_youtube_token.py --channel congreso-es-tv --purpose analytics
```

Then copy the whole `youtube_tokens/` tree to the Airflow data directory
(`/opt/airflow/data/congress_videos/youtube_tokens/`).

## Onboarding a new channel

1. Add a `ChannelConfig` entry to `CHANNELS` in `youtube_channels.py`.
2. `mkdir youtube_tokens/{new-channel}` (add a `.gitkeep`).
3. Generate its tokens with `--channel {new-channel}`.

## Backward compatibility

Until a channel's tokens are generated, `resolve_token_path()` falls back to the
legacy `congress_youtube_token.pickle` for the default channel's `upload`
purpose only. The `analytics` purpose never falls back — the legacy token lacks
the `yt-analytics.readonly` scope.
