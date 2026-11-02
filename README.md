# 大戦闘先東ブラザーズ

Cloudflare Workers + Durable Objects + Static Assets で動作するゲームです。

## ファイル構成

- `worker.js` — WebSocket とゲームルームの処理
- `wrangler.toml` — Workers と Durable Objects の設定
- `public/index.html` — 画面
- `public/game-hero.png` — トップ画像
- `public/oni1.jpg` ～ `public/oni7.jpg` — 鬼の画像
- `public/final-face.jpg` — 終了画像

## デプロイ

このZIPは**GitHubに展開して登録するソース一式**です。ZIPそのものをGitHubへ1ファイルとして置いてもCloudflareは展開しません。GitHubのリポジトリ直下に `worker.js`、`wrangler.toml`、`public/` を登録してください。

Cloudflare Workers でこのリポジトリを接続し、Wranglerのデプロイコマンド `npx wrangler deploy` を使用してください。既存の Cloudflare Pages `105game.pages.dev` とは別のテスト用Workerに公開します。

Workersの実環境・複数端末での同期動作は未検証です。既存Pagesの更新には使用しないでください。
