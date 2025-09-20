# Waylis Sqlite DB

Node.js Sqlite implementation of a Database interface for [Waylis](https://github.com/waylis/core). Build using [better-sqlite3](https://github.com/WiseLibs/better-sqlite3).

```sh
npm install @waylis/sqlite-db
```

```ts
import { SqliteDatabase } from "@waylis/sqlite-db";
```

```ts
const app = new AppServer({
    db: new SqliteDatabase("sqlite.db"),
});
```
