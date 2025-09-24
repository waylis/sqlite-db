import type { Chat, ConfirmedStep, FileMeta, Message, Database } from "@waylis/shared";
import Sqlite from "better-sqlite3";

export class SqliteDatabase implements Database {
    private dbPath: string;
    private db?: Sqlite.Database;
    private pragmas: string[] = [
        "journal_mode = WAL",
        "synchronous = NORMAL",
        "cache_size = -2000", // ~2k pages ~ default page size 1024 => ~2MB cache
        "busy_timeout = 5000", // 5s busy timeout
    ];
    isOpen = false;

    private stmts: { [k: string]: any } = {};

    constructor(dbPath: string, pragmas?: string[]) {
        this.dbPath = dbPath;
        if (pragmas?.length) this.pragmas = pragmas;
    }

    async open(): Promise<void> {
        if (this.isOpen) return;
        const db = new Sqlite(this.dbPath, { fileMustExist: false });

        for (const pragma of this.pragmas) {
            db.pragma(pragma);
        }

        this.db = db;
        this.isOpen = true;

        const migrations = [
            `CREATE TABLE IF NOT EXISTS chats (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                creatorID TEXT NOT NULL,
                createdAt INTEGER NOT NULL
            );`,
            `CREATE INDEX IF NOT EXISTS idx_chats_creator_createdAt ON chats(creatorID, createdAt);`,

            `CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                chatID TEXT NOT NULL,
                senderID TEXT NOT NULL,
                replyTo TEXT,
                threadID TEXT,
                scene TEXT,
                step TEXT,
                body TEXT NOT NULL, -- JSON
                replyRestriction TEXT, -- JSON
                createdAt INTEGER NOT NULL
            );`,
            `CREATE INDEX IF NOT EXISTS idx_messages_chat_createdAt ON messages(chatID, createdAt DESC, id DESC);`,

            `CREATE TABLE IF NOT EXISTS confirmed_steps (
                id TEXT PRIMARY KEY,
                threadID TEXT NOT NULL,
                messageID TEXT NOT NULL,
                scene TEXT NOT NULL,
                step TEXT NOT NULL,
                createdAt INTEGER NOT NULL
            );`,
            `CREATE INDEX IF NOT EXISTS idx_confirmed_steps_thread_createdAt ON confirmed_steps(threadID, createdAt);`,

            `CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                mimeType TEXT NOT NULL,
                createdAt INTEGER NOT NULL
            );`,
            `CREATE INDEX IF NOT EXISTS idx_files_createdAt ON files(createdAt);`,
        ];

        const exec = db.exec.bind(db);
        db.transaction(() => {
            for (const sql of migrations) exec(sql);
        })();

        this.prepareStatements();
    }

    private prepareStatements() {
        if (!this.db) throw new Error("DB not open");

        const db = this.db;

        // chats
        this.stmts.addChat = db.prepare(`INSERT INTO chats (id, name, creatorID, createdAt) VALUES (?, ?, ?, ?)`);
        this.stmts.getChatByID = db.prepare(`SELECT * FROM chats WHERE id = ?`);
        this.stmts.getChatsByCreatorID = db.prepare(
            `SELECT * FROM chats WHERE creatorID = ? ORDER BY createdAt DESC LIMIT ? OFFSET ?`
        );
        this.stmts.countChatsByCreatorID = db.prepare(`SELECT COUNT(1) as c FROM chats WHERE creatorID = ?`);
        this.stmts.editChatByID = db.prepare(
            `UPDATE chats SET name = coalesce(?, name), creatorID = coalesce(?, creatorID), createdAt = coalesce(?, createdAt) WHERE id = ?`
        );
        this.stmts.deleteChatByID = db.prepare(
            `DELETE FROM chats WHERE id = ? RETURNING id, name, creatorID, createdAt`
        );

        // messages
        this.stmts.addMessage = db.prepare(
            `INSERT INTO messages(id, chatID, senderID, replyTo, threadID, scene, step, body, replyRestriction, createdAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        );
        this.stmts.getMessageByID = db.prepare(`SELECT * FROM messages WHERE id = ?`);
        this.stmts.getMessagesByChatID = db.prepare(
            `SELECT * FROM messages WHERE chatID = ? ORDER BY createdAt DESC, id DESC LIMIT ? OFFSET ?`
        );
        this.stmts.deleteOldMessages = db.prepare(`DELETE FROM messages WHERE createdAt < ?`);
        this.stmts.deleteMessagesByChatID = db.prepare(`DELETE FROM messages WHERE chatID = ?`);

        // confirmed steps
        this.stmts.addConfirmedStep = db.prepare(
            `INSERT INTO confirmed_steps(id, threadID, messageID, scene, step, createdAt) VALUES (?, ?, ?, ?, ?, ?)`
        );
        this.stmts.getConfirmedStepsByThreadID = db.prepare(
            `SELECT * FROM confirmed_steps WHERE threadID = ? ORDER BY createdAt DESC`
        );
        this.stmts.deleteOldConfirmedSteps = db.prepare(`DELETE FROM confirmed_steps WHERE createdAt < ?`);

        // files
        this.stmts.addFile = db.prepare(
            `INSERT INTO files(id, name, size, mimeType, createdAt) VALUES (?, ?, ?, ?, ?)`
        );
        this.stmts.getFileByID = db.prepare(`SELECT * FROM files WHERE id = ?`);
        this.stmts.deleteFileByID = db.prepare(
            `DELETE FROM files WHERE id = ? RETURNING id, name, size, mimeType, createdAt`
        );
        this.stmts.deleteOldFiles = db.prepare(`SELECT id FROM files WHERE createdAt < ?`);
    }

    private rowToChat(row: any): Chat {
        return {
            id: row.id,
            name: row.name,
            creatorID: row.creatorID,
            createdAt: new Date(row.createdAt),
        };
    }

    private rowToMessage(row: any): Message {
        return {
            id: row.id,
            chatID: row.chatID,
            senderID: row.senderID,
            replyTo: row.replyTo ?? undefined,
            threadID: row.threadID,
            scene: row.scene ?? undefined,
            step: row.step ?? undefined,
            body: JSON.parse(row.body),
            replyRestriction: row.replyRestriction ? JSON.parse(row.replyRestriction) : undefined,
            createdAt: new Date(row.createdAt),
        };
    }

    private rowToConfirmedStep(row: any): ConfirmedStep {
        return {
            id: row.id,
            threadID: row.threadID,
            messageID: row.messageID,
            scene: row.scene,
            step: row.step,
            createdAt: new Date(row.createdAt),
        };
    }

    private rowToFile(row: any): FileMeta {
        return {
            id: row.id,
            name: row.name,
            size: row.size,
            mimeType: row.mimeType,
            createdAt: new Date(row.createdAt),
        };
    }

    async close(): Promise<void> {
        if (!this.db || !this.isOpen) return;
        this.db.close();
        this.isOpen = false;
        this.db = undefined;
    }

    // ChatDatabase
    async addChat(chat: Chat): Promise<void> {
        if (!this.db) throw new Error("DB closed");
        this.stmts.addChat.run(chat.id, chat.name, chat.creatorID, chat.createdAt.getTime());
    }

    async getChatByID(id: string): Promise<Chat | null> {
        if (!this.db) throw new Error("DB closed");
        const row = this.stmts.getChatByID.get(id);
        return row ? this.rowToChat(row) : null;
    }

    async getChatsByCreatorID(creatorID: string, offset = 0, limit = 50): Promise<Chat[]> {
        if (!this.db) throw new Error("DB closed");
        const rows = this.stmts.getChatsByCreatorID.all(creatorID, limit, offset);
        return rows.map((r: any) => this.rowToChat(r));
    }

    async countChatsByCreatorID(creatorID: string): Promise<number> {
        if (!this.db) throw new Error("DB closed");
        const row = this.stmts.countChatsByCreatorID.get(creatorID);
        return row ? (row.c as number) : 0;
    }

    async editChatByID(id: string, updated: Partial<Chat>): Promise<Chat | null> {
        if (!this.db) throw new Error("DB closed");
        const createdAt = updated.createdAt ? updated.createdAt.getTime() : null;
        this.stmts.editChatByID.run(updated.name ?? null, updated.creatorID ?? null, createdAt, id);
        return this.getChatByID(id);
    }

    async deleteChatByID(id: string): Promise<Chat | null> {
        if (!this.db) throw new Error("DB closed");
        try {
            const row = this.stmts.deleteChatByID.get(id);
            return row ? this.rowToChat(row) : null;
        } catch {
            // fallback: select then delete
            const chat = await this.getChatByID(id);
            if (!chat) return null;
            this.db!.prepare(`DELETE FROM chats WHERE id = ?`).run(id);
            return chat;
        }
    }

    // MessageDatabase
    async addMessage(msg: Message): Promise<void> {
        if (!this.db) throw new Error("DB closed");
        this.stmts.addMessage.run(
            msg.id,
            msg.chatID,
            msg.senderID,
            msg.replyTo ?? null,
            msg.threadID ?? null,
            msg.scene ?? null,
            msg.step ?? null,
            JSON.stringify(msg.body),
            msg.replyRestriction ? JSON.stringify(msg.replyRestriction) : null,
            msg.createdAt.getTime()
        );
    }

    async getMessageByID(id: string): Promise<Message | null> {
        if (!this.db) throw new Error("DB closed");
        const row = this.stmts.getMessageByID.get(id);
        return row ? this.rowToMessage(row) : null;
    }

    async getMessagesByIDs(ids: string[]): Promise<Message[]> {
        if (!this.db) throw new Error("DB closed");
        if (ids.length === 0) return [];

        const placeholders = ids.map(() => "?").join(",");
        const sql = `SELECT * FROM messages WHERE id IN (${placeholders})`;
        const rows = this.db!.prepare(sql).all(...ids);
        return rows.map((r: any) => this.rowToMessage(r));
    }

    async getMessagesByChatID(chatID: string, offset = 0, limit = 100): Promise<Message[]> {
        if (!this.db) throw new Error("DB closed");
        const rows = this.stmts.getMessagesByChatID.all(chatID, limit, offset);
        return rows.map((r: any) => this.rowToMessage(r));
    }

    async deleteOldMessages(maxDate: Date): Promise<number> {
        if (!this.db) throw new Error("DB closed");
        const res = this.stmts.deleteOldMessages.run(maxDate.getTime());
        return res.changes as number;
    }

    async deleteMessagesByChatID(chatID: string): Promise<number> {
        if (!this.db) throw new Error("DB closed");
        const res = this.stmts.deleteMessagesByChatID.run(chatID);
        return res.changes as number;
    }

    // ConfirmedStepDatabase
    async addConfirmedStep(step: ConfirmedStep): Promise<void> {
        if (!this.db) throw new Error("DB closed");
        this.stmts.addConfirmedStep.run(
            step.id,
            step.threadID,
            step.messageID,
            step.scene,
            step.step,
            step.createdAt.getTime()
        );
    }

    async getConfirmedStepsByThreadID(threadID: string): Promise<ConfirmedStep[]> {
        if (!this.db) throw new Error("DB closed");
        const rows = this.stmts.getConfirmedStepsByThreadID.all(threadID);
        return rows.map((r: any) => this.rowToConfirmedStep(r));
    }

    async deleteOldConfirmedSteps(maxDate: Date): Promise<number> {
        if (!this.db) throw new Error("DB closed");
        const res = this.stmts.deleteOldConfirmedSteps.run(maxDate.getTime());
        return res.changes as number;
    }

    // FileDatabase
    async addFile(data: FileMeta): Promise<void> {
        if (!this.db) throw new Error("DB closed");
        this.stmts.addFile.run(data.id, data.name, data.size, data.mimeType, data.createdAt.getTime());
    }

    async getFileByID(id: string): Promise<FileMeta | null> {
        if (!this.db) throw new Error("DB closed");
        const row = this.stmts.getFileByID.get(id);
        return row ? this.rowToFile(row) : null;
    }

    async getFilesByIDs(ids: string[]): Promise<FileMeta[]> {
        if (!this.db) throw new Error("DB closed");
        if (ids.length === 0) return [];
        const placeholders = ids.map(() => "?").join(",");
        const sql = `SELECT * FROM files WHERE id IN (${placeholders})`;
        const rows = this.db!.prepare(sql).all(...ids);
        return rows.map((r: any) => this.rowToFile(r));
    }

    async deleteFileByID(id: string): Promise<FileMeta | null> {
        if (!this.db) throw new Error("DB closed");
        try {
            const row = this.stmts.deleteFileByID.get(id);
            return row ? this.rowToFile(row) : null;
        } catch {
            const file = await this.getFileByID(id);
            if (!file) return null;
            this.db!.prepare(`DELETE FROM files WHERE id = ?`).run(id);
            return file;
        }
    }

    async deleteOldFiles(maxDate: Date): Promise<string[]> {
        if (!this.db) throw new Error("DB closed");
        const rows = this.stmts.deleteOldFiles.all(maxDate.getTime());
        const ids = rows.map((r: any) => r.id as string);

        if (ids.length) {
            const placeholders = ids.map(() => "?").join(",");
            this.db!.prepare(`DELETE FROM files WHERE id IN (${placeholders})`).run(...ids);
        }
        return ids;
    }
}
